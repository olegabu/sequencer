#pragma once

// Ties together the tailing thread, the resume position, the broadcast
// ring, the transport, and the codec — everything RunOutputGateway
// (output_gateway.hpp) sets up, minus argv/gflags parsing, exactly as
// node/'s NodeImpl is separated from RunNode.
//
// Delivery design (see include/sequencer/broadcast_ring.hpp's file
// comment for the full rationale): the one tailing thread here runs
// the codec exactly once per journal record, in order — §8.3's
// contract — publishing each output into the chassis-owned
// BroadcastRing. Each connected subscriber then drains the ring
// through its own private cursor on its own thread, inside the
// transport. The previous design instead pushed every record through
// per-session queues + a cross-thread write hand-off, which is where
// multi-millisecond delivery latency measurably accumulated
// (examples/counter/README.md's benchmark section).

#include <atomic>
#include <chrono>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <sequencer/broadcast_ring.hpp>
#include <sequencer/journal/reader.hpp>
#include <sequencer/output_codec.hpp>
#include <sequencer/output_transport.hpp>

#include "resume_position.hpp"

namespace sequencer::gateway::output::detail {

struct OutputGatewayConfig {
  std::filesystem::path dataDir;     // a node's journal directory, colocated (§3)
  std::filesystem::path resumeFile;  // durable resume position
  // No listen port here: a port belongs to a transport, and this
  // gateway drives a list of them (see the constructor). One journal
  // tail, one codec pass, one ring — N protocols on N ports.
  // BroadcastRing sizing. 65536 slots of (up to) 512 bytes = 32MB —
  // ~650ms of headroom at 100k records/sec before a completely stalled
  // reader is overrun and disconnected. A codec output larger than
  // ringMaxPayload makes publish() throw, so size this for the
  // application's actual outputs.
  std::size_t ringSlots = 65536;
  std::size_t ringMaxPayload = 512;
  // How long the tailing thread busy-spins on the journal before
  // backing off when fully caught up — the relay's own idle idiom
  // (sub-millisecond common case), replacing this loop's original
  // flat 5ms sleep, which put up to 5ms of pure polling delay ahead
  // of every record that arrived into an idle gateway.
  int idleSpinIterations = 1000;
};

class OutputGatewayImpl {
 public:
  // One transport bound to the port it should listen on.
  struct Binding {
    std::unique_ptr<sequencer::OutputTransport> transport;
    int listenPort = 0;
  };

  // Any number of transports share this gateway's single journal tail,
  // single codec pass and single ring — the ring publishes each record
  // once no matter how many transports are attached, so serving three
  // protocols costs the producer exactly what serving one does. That
  // is the whole reason this takes a list (see
  // gateway/output/README.md, "Delivery: one ring, one reader per
  // subscriber"); under the old push design N transports would have
  // meant N per-record hand-offs.
  OutputGatewayImpl(OutputGatewayConfig config, std::unique_ptr<sequencer::OutputCodec> codec,
                     std::vector<Binding> bindings)
      : config_(std::move(config)),
        codec_(std::move(codec)),
        bindings_(std::move(bindings)),
        resumePosition_(config_.resumeFile),
        ring_(config_.ringSlots, config_.ringMaxPayload),
        ringFanout_(ring_, topics_) {}

  // Single-transport convenience, for callers (and tests) that only
  // ever want one.
  OutputGatewayImpl(OutputGatewayConfig config, std::unique_ptr<sequencer::OutputCodec> codec,
                     std::unique_ptr<sequencer::OutputTransport> transport, int listenPort)
      : OutputGatewayImpl(std::move(config), std::move(codec),
                           makeSingleton(std::move(transport), listenPort)) {}

  OutputGatewayImpl(const OutputGatewayImpl&) = delete;
  OutputGatewayImpl& operator=(const OutputGatewayImpl&) = delete;

  // A still-joinable std::thread member calls std::terminate() on
  // destruction — matters here because a fatal test assertion (or any
  // exception) can unwind past an explicit stop() call.
  ~OutputGatewayImpl() {
    if (started_) {
      stop();
    }
  }

  void start() {
    for (Binding& b : bindings_) {
      b.transport->attach(ring_, topics_, config_.idleSpinIterations);
      b.transport->start(b.listenPort);
    }
    tailThread_ = std::thread([this] { tailLoop(); });
    started_ = true;
  }

  void stop() {
    stopRequested_.store(true, std::memory_order_relaxed);
    if (tailThread_.joinable()) {
      tailThread_.join();
    }
    // The tailing thread only persists periodically now (see
    // tailLoop()'s own comment) — one final, unconditional persist
    // here is what keeps ResumesFromDurablePositionAfterRestart
    // WithoutRedelivering's own guarantee exactly true for a graceful
    // stop(), even though a batched write cadence means an ungraceful
    // crash could now redeliver up to one batch's worth of records.
    resumePosition_.store(nextSeq_);
    // Transports last: their per-subscriber readers drain the ring
    // directly, so everything published before this point is still
    // deliverable until stop() disconnects them.
    for (Binding& b : bindings_) {
      b.transport->stop();
    }
    started_ = false;
  }

 private:
  // The codec publishes into the ring, never touches a transport —
  // Fanout's toSession/broadcast become tagged ring publishes
  // (broadcast_ring.hpp's tag encoding; readers filter by these).
  class RingFanout final : public sequencer::Fanout {
   public:
    RingFanout(sequencer::BroadcastRing& ring, sequencer::TopicRegistry& topics)
        : ring_(ring), topics_(topics) {}

    void toSession(sequencer::SessionId owner, Bytes bytes) override {
      ring_.publish(sequencer::makeSessionTag(owner), bytes.data(), bytes.size());
    }

    void broadcast(const std::string& topic, Bytes bytes) override {
      ring_.publish(sequencer::makeTopicTag(topics_.idFor(topic)), bytes.data(), bytes.size());
    }

   private:
    sequencer::BroadcastRing& ring_;
    sequencer::TopicRegistry& topics_;
  };

  void tailLoop() {
    std::uint64_t seq = resumePosition_.load();
    nextSeq_.store(seq, std::memory_order_relaxed);
    std::unique_ptr<journal::JournalReader> reader;
    std::uint64_t persistedThrough = seq;
    auto lastPersist = std::chrono::steady_clock::now();
    sequencer::IdleStrategy idle(config_.idleSpinIterations);
    while (!stopRequested_.load(std::memory_order_relaxed)) {
      if (!reader) {
        try {
          reader = std::make_unique<journal::JournalReader>(config_.dataDir / "journal");
        } catch (const std::exception&) {
          // The journal file pair may not exist yet — retry.
          std::this_thread::sleep_for(std::chrono::milliseconds(50));
          continue;
        }
      }
      if (!reader->contains(seq)) {
        idle.idle();
        continue;
      }
      idle.reset();

      // One codec call per record, strictly in order — §8.3's "in
      // order, exactly once" holds by construction; each call
      // publishes into the ring, and every subscriber's own reader
      // batches naturally by draining whatever range accumulated
      // since its last drain. The bounded burst here just keeps the
      // resume-position bookkeeping below running regularly during a
      // long backlog catch-up.
      constexpr int kMaxBurst = 1024;
      int processed = 0;
      while (processed < kMaxBurst && reader->contains(seq) &&
             !stopRequested_.load(std::memory_order_relaxed)) {
        codec_->toOutput(reader->record(seq), ringFanout_);
        ++seq;
        ++processed;
        nextSeq_.store(seq, std::memory_order_relaxed);
      }

      // Persisting the resume position is not free — ResumePosition::
      // store() is a full open/write/close/rename cycle, several real
      // filesystem syscalls — and doing that after literally every
      // record made this the dominant cost of the whole tailing loop,
      // reproduced live: an output gateway couldn't sustain even
      // 10k records/sec of dissemination, falling permanently behind
      // and never catching up within an entire benchmark run (see
      // bench/load_generator/README.md's "four round trips" section
      // and examples/counter/README.md's own account of finding this).
      // Batched instead: persist at most every 1000 records or 200ms,
      // whichever comes first — a graceful stop() (above) always does
      // one final, unconditional persist, so ResumesFromDurablePosition
      // AfterRestartWithoutRedelivering's own guarantee stays exactly
      // true for a normal restart; only an ungraceful crash can now
      // redeliver, bounded to at most one batch's worth of records.
      const auto now = std::chrono::steady_clock::now();
      if (seq - persistedThrough >= 1000 || now - lastPersist >= std::chrono::milliseconds(200)) {
        resumePosition_.store(seq);
        persistedThrough = seq;
        lastPersist = now;
      }
    }
  }

  static std::vector<Binding> makeSingleton(std::unique_ptr<sequencer::OutputTransport> transport,
                                             int listenPort) {
    std::vector<Binding> v;
    v.push_back(Binding{std::move(transport), listenPort});
    return v;
  }

  OutputGatewayConfig config_;
  std::unique_ptr<sequencer::OutputCodec> codec_;
  std::vector<Binding> bindings_;
  ResumePosition resumePosition_;
  sequencer::TopicRegistry topics_;
  sequencer::BroadcastRing ring_;
  RingFanout ringFanout_;
  std::atomic<std::uint64_t> nextSeq_{1};  // mirrors tailLoop()'s own seq, readable from stop()
  std::thread tailThread_;
  std::atomic<bool> stopRequested_{false};
  bool started_ = false;
};

}  // namespace sequencer::gateway::output::detail
