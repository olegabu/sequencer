#pragma once

// Ties together the tailing thread, the resume position, the transport,
// and the codec — everything RunOutputGateway (output_gateway.hpp) sets
// up, minus argv/gflags parsing, exactly as node/'s NodeImpl is
// separated from RunNode.

#include <atomic>
#include <chrono>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>

#include <sequencer/journal/reader.hpp>
#include <sequencer/output_codec.hpp>
#include <sequencer/output_transport.hpp>

#include "resume_position.hpp"

namespace sequencer::gateway::output::detail {

struct OutputGatewayConfig {
  std::filesystem::path dataDir;     // a node's journal directory, colocated (§3)
  std::filesystem::path resumeFile;  // durable resume position
  int listenPort = 0;
};

class OutputGatewayImpl {
 public:
  OutputGatewayImpl(OutputGatewayConfig config, std::unique_ptr<sequencer::OutputCodec> codec,
                     std::unique_ptr<sequencer::OutputTransport> transport)
      : config_(std::move(config)),
        codec_(std::move(codec)),
        transport_(std::move(transport)),
        resumePosition_(config_.resumeFile) {}

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
    transport_->start(config_.listenPort);
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
    // Same reasoning for transport_->flush(): tailLoop() only flushes
    // after a batch, not per record now, so a graceful stop() must
    // flush once more itself or the last partial batch's worth of
    // already-codec'd records would sit forever in a transport's
    // internal buffer, never actually sent.
    transport_->flush();
    transport_->stop();
    started_ = false;
  }

  int listenPort() const { return config_.listenPort; }

 private:
  void tailLoop() {
    std::uint64_t seq = resumePosition_.load();
    nextSeq_.store(seq, std::memory_order_relaxed);
    std::unique_ptr<journal::JournalReader> reader;
    std::uint64_t persistedThrough = seq;
    auto lastPersist = std::chrono::steady_clock::now();
    while (!stopRequested_.load(std::memory_order_relaxed)) {
      if (!reader) {
        try {
          reader = std::make_unique<journal::JournalReader>(config_.dataDir / "journal.data",
                                                              config_.dataDir / "journal.index");
        } catch (const std::exception&) {
          // The journal file pair may not exist yet — retry.
          std::this_thread::sleep_for(std::chrono::milliseconds(50));
          continue;
        }
      }
      if (!reader->contains(seq)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        continue;
      }

      // Gathers everything already available (up to a cap) before
      // flushing the transport once — never delaying to wait for
      // more, so a caught-up gateway still flushes after exactly one
      // record. specification.md §8.3: "in sequence-number order...
      // in order, exactly once" still holds — one codec call per
      // record, strictly in order; only the *transport's own* write
      // round trip is now batched, not the codec invocation. This
      // exists for the same reason as the relay's own gRPC Subscribe
      // batching (gateway/relay/README.md's "Batching the gRPC
      // stream" section): live-fleet profiling of this tailing loop
      // found the dominant cost was one unbatched transport write per
      // record — see StreamFanout::flush()'s own comment for the
      // brpc-specific numbers this was measured against.
      constexpr int kMaxBatch = 1024;
      int gathered = 0;
      while (gathered < kMaxBatch && reader->contains(seq) && !stopRequested_.load(std::memory_order_relaxed)) {
        codec_->toOutput(reader->record(seq), *transport_);
        ++seq;
        ++gathered;
        nextSeq_.store(seq, std::memory_order_relaxed);
      }
      transport_->flush();

      // Persisting the resume position is not free — ResumePosition::
      // store() is a full open/write/close/rename cycle, several real
      // filesystem syscalls — and doing that after literally every
      // record made this the dominant cost of the whole tailing loop,
      // reproduced live: an output gateway couldn't sustain even
      // 10k records/sec of dissemination, falling permanently behind
      // and never catching up within an entire benchmark run (see
      // bench/load_generator/README.md's "four round trips" section
      // and examples/counter/README.md's own account of finding this).
      // Batched instead, the same "asynchronously flushed by default"
      // choice this file's own top comment already describes for the
      // journal but this method wasn't actually following: persist at
      // most every 1000 records or 200ms, whichever comes first — a
      // graceful stop() (below) always does one final, unconditional
      // persist, so ResumesFromDurablePositionAfterRestartWithout
      // Redelivering's own guarantee stays exactly true for a normal
      // restart; only an ungraceful crash can now redeliver, bounded
      // to at most one batch's worth of records.
      const auto now = std::chrono::steady_clock::now();
      if (seq - persistedThrough >= 1000 || now - lastPersist >= std::chrono::milliseconds(200)) {
        resumePosition_.store(seq);
        persistedThrough = seq;
        lastPersist = now;
      }
    }
  }

  OutputGatewayConfig config_;
  std::unique_ptr<sequencer::OutputCodec> codec_;
  std::unique_ptr<sequencer::OutputTransport> transport_;
  ResumePosition resumePosition_;
  std::atomic<std::uint64_t> nextSeq_{1};  // mirrors tailLoop()'s own seq, readable from stop()
  std::thread tailThread_;
  std::atomic<bool> stopRequested_{false};
  bool started_ = false;
};

}  // namespace sequencer::gateway::output::detail
