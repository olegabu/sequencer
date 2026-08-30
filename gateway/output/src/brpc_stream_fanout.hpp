#pragma once

// The brpc-side subscriber registry: accepts Subscribe streams and
// gives each one its own reader thread draining the chassis's
// BroadcastRing through a private cursor (see include/sequencer/
// broadcast_ring.hpp's file comment for the whole delivery design —
// this replaced a push-based pending-buffer scheme whose per-session
// queueing was where multi-millisecond delivery latency measurably
// accumulated; examples/counter/README.md's benchmark section).
//
// Wire format unchanged from that earlier scheme: brpc::Stream is
// message-oriented, not a raw byte stream — on_received_messages()
// delivers whatever one StreamWrite() call sent as one opaque unit,
// so a batch of N payloads is framed as N (4-byte big-endian length,
// payload) pairs in one message. Deliberately not a protobuf envelope
// (unlike GrpcOutputTransport's OutputRecordBatch): this transport's
// whole point, per specification.md §8.7, is carrying an
// OutputCodec's bytes completely unmodified for consumers with no
// protobuf dependency of their own. bench/load_generator's brpc
// observer and gateway/output/tests/collecting_stream_client.hpp both
// decode it; any other brpc client of this transport needs to as well.

#include <brpc/stream.h>
#include <butil/iobuf.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <sequencer/broadcast_ring.hpp>
#include <sequencer/output_codec.hpp>

namespace sequencer::gateway::output::detail {

class BrpcStreamFanout : public brpc::StreamInputHandler {
 public:
  void attach(sequencer::BroadcastRing& ring, sequencer::TopicRegistry& topics, int idleSpinIterations) {
    ring_ = &ring;
    topics_ = &topics;
    idleSpinIterations_ = idleSpinIterations;
  }

  sequencer::SessionId nextSessionId() { return nextSessionId_.fetch_add(1, std::memory_order_relaxed); }

  // Registers a newly-accepted stream under `sessionId`, joined to
  // `topic`, and starts its reader thread. Called by
  // BrpcSubscribeServiceImpl right after brpc::StreamAccept — i.e.
  // before the Subscribe RPC's response is sent, which is the ordering
  // clients rely on: anything published after subscribe() returns must
  // be delivered. The cursor is therefore captured HERE, on this
  // thread, not inside the reader thread — a freshly-spawned thread
  // can be scheduled arbitrarily late, and a head() read that happens
  // only then would silently skip everything published in between as
  // pre-subscription history (a real, reproduced test flake).
  void registerStream(brpc::StreamId streamId, sequencer::SessionId sessionId, const std::string& topic) {
    auto reader = std::make_unique<Reader>();
    Reader* readerPtr = reader.get();
    const std::uint64_t topicTag = sequencer::makeTopicTag(topics_->idFor(topic));
    const std::uint64_t sessionTag = sequencer::makeSessionTag(sessionId);
    const std::uint64_t initialCursor = ring_->head();
    // Thread started and map entry inserted under one lock hold: an
    // on_closed() firing for this very stream must find the entry
    // complete (thread joinable) or not find it at all — never a
    // half-registered Reader it would join-skip and then free out
    // from under this method.
    std::lock_guard<std::mutex> lock(mutex_);
    readerPtr->thread = std::thread([this, streamId, topicTag, sessionTag, initialCursor, readerPtr] {
      readLoop(streamId, topicTag, sessionTag, initialCursor, *readerPtr);
    });
    readers_[streamId] = std::move(reader);
  }

  // Force-closes every currently-registered stream and waits for each
  // one's on_closed() to actually fire — and its reader thread to be
  // joined — before returning.
  //
  // Both halves matter. brpc::Server::Join() waits for its acceptor to
  // observe every accepted connection fully closed, so a client that
  // never voluntarily disconnects its Subscribe stream would otherwise
  // hang shutdown indefinitely — that's what StreamClose() is for.
  // But StreamClose() does not itself guarantee on_closed() has already
  // run by the time it returns; on_closed() can fire asynchronously,
  // from a different thread, arbitrarily shortly after. If this object
  // is destroyed before that callback lands, on_closed() runs against
  // freed memory — a real, rare, hard-to-reproduce crash this method
  // exists specifically to close off. The wait (bounded, so a
  // pathological stream can never hang shutdown forever) is what turns
  // "StreamClose() was called" into "it is now safe to destroy this
  // object." Called by BrpcOutputTransport::stop() before stopping the
  // server — and, transitively, before anything owning this fanout can
  // be destroyed.
  void closeAll() {
    std::unique_lock<std::mutex> lock(mutex_);
    std::vector<brpc::StreamId> streams;
    streams.reserve(readers_.size());
    for (const auto& [streamId, reader] : readers_) {
      (void)reader;
      streams.push_back(streamId);
    }
    pendingClose_.insert(streams.begin(), streams.end());
    lock.unlock();

    for (brpc::StreamId streamId : streams) {
      brpc::StreamClose(streamId);
    }

    lock.lock();
    closedCv_.wait_for(lock, std::chrono::seconds(5), [this] { return pendingClose_.empty(); });
    // Anything still pending after the bounded wait gets its reader
    // stopped and joined here regardless — the reader threads are
    // this object's own and must not outlive it even if brpc never
    // delivers an on_closed().
    std::vector<std::unique_ptr<Reader>> leftovers;
    for (auto& [streamId, reader] : readers_) {
      (void)streamId;
      leftovers.push_back(std::move(reader));
    }
    readers_.clear();
    lock.unlock();
    for (auto& reader : leftovers) {
      stopAndJoin(*reader);
    }
  }

  // brpc::StreamInputHandler — this fanout only ever writes; clients
  // never send anything over their subscribe stream.
  int on_received_messages(brpc::StreamId, butil::IOBuf* const*, size_t) override { return 0; }
  void on_idle_timeout(brpc::StreamId) override {}
  void on_closed(brpc::StreamId streamId) override { deregister(streamId); }

 private:
  struct Reader {
    std::thread thread;
    std::atomic<bool> stop{false};
  };

  // One subscriber's whole delivery path, on its own thread: drain
  // everything available from a private cursor, filter by tag, frame,
  // one StreamWrite per drained batch; spin-then-back-off when caught
  // up. Overrun (lapped by the producer — this reader is genuinely
  // not keeping up) closes the stream rather than silently skipping:
  // surfacing the slow consumer is the contract (broadcast_ring.hpp).
  void readLoop(brpc::StreamId streamId, std::uint64_t topicTag, std::uint64_t sessionTag,
                std::uint64_t initialCursor, Reader& reader) {
    std::uint64_t cursor = initialCursor;
    std::vector<std::byte> payload(ring_->maxPayload());
    sequencer::IdleStrategy idle(idleSpinIterations_);
    constexpr int kMaxBatch = 1024;
    while (!reader.stop.load(std::memory_order_relaxed)) {
      butil::IOBuf batch;
      int gathered = 0;
      bool overrun = false;
      while (gathered < kMaxBatch) {
        std::uint64_t tag = 0;
        std::uint32_t length = 0;
        const auto result = ring_->readOne(cursor, tag, payload.data(), length);
        if (result == sequencer::BroadcastRing::ReadResult::Empty) {
          break;
        }
        if (result == sequencer::BroadcastRing::ReadResult::Overrun) {
          overrun = true;
          break;
        }
        if (tag != topicTag && tag != sessionTag) {
          continue;  // someone else's entry; not counted against the batch cap
        }
        std::uint8_t lengthPrefix[4];
        lengthPrefix[0] = static_cast<std::uint8_t>(length >> 24);
        lengthPrefix[1] = static_cast<std::uint8_t>(length >> 16);
        lengthPrefix[2] = static_cast<std::uint8_t>(length >> 8);
        lengthPrefix[3] = static_cast<std::uint8_t>(length);
        batch.append(lengthPrefix, sizeof(lengthPrefix));
        batch.append(payload.data(), length);
        ++gathered;
      }
      if (!batch.empty()) {
        write(streamId, batch);
        idle.reset();
      }
      if (overrun) {
        brpc::StreamClose(streamId);  // on_closed() deregisters and joins
        return;
      }
      if (batch.empty()) {
        idle.idle();
      }
    }
  }

  // Bounded retry on EAGAIN — the remote hasn't drained enough of what
  // was already sent, expected backpressure under real throughput, not
  // a failure. A single-shot attempt silently and permanently dropped
  // the message whenever that happened, which turned out to mean
  // *almost every* message under real load (examples/counter/
  // README.md's "four round trips" section). ~10ms total: this now
  // runs on the one subscriber's OWN reader thread — nobody else is
  // behind it — but an overrun-disconnect is the design's slow-
  // consumer answer, so blocking much longer here just delays it.
  void write(brpc::StreamId streamId, const butil::IOBuf& buf) {
    for (int attempt = 0; attempt < 50; ++attempt) {
      const int rc = brpc::StreamWrite(streamId, buf);
      if (rc == 0) {
        return;
      }
      if (errno != EAGAIN) {
        return;  // the client is gone; on_closed() will deregister it shortly if it hasn't already
      }
      std::this_thread::sleep_for(std::chrono::microseconds(200));
    }
  }

  void stopAndJoin(Reader& reader) {
    reader.stop.store(true, std::memory_order_relaxed);
    if (reader.thread.joinable()) {
      // on_closed() (and thus this) can be invoked from the reader's
      // own StreamClose() call after an overrun — never self-join.
      if (reader.thread.get_id() == std::this_thread::get_id()) {
        reader.thread.detach();
      } else {
        reader.thread.join();
      }
    }
  }

  void deregister(brpc::StreamId streamId) {
    std::unique_ptr<Reader> reader;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      const auto it = readers_.find(streamId);
      if (it != readers_.end()) {
        reader = std::move(it->second);
        readers_.erase(it);
      }
      if (pendingClose_.erase(streamId) > 0 && pendingClose_.empty()) {
        closedCv_.notify_all();
      }
    }
    if (reader) {
      stopAndJoin(*reader);
    }
  }

  sequencer::BroadcastRing* ring_ = nullptr;
  sequencer::TopicRegistry* topics_ = nullptr;
  int idleSpinIterations_ = 1000;

  std::mutex mutex_;
  std::condition_variable closedCv_;
  std::unordered_map<brpc::StreamId, std::unique_ptr<Reader>> readers_;
  std::unordered_set<brpc::StreamId> pendingClose_;
  std::atomic<sequencer::SessionId> nextSessionId_{1};
};

}  // namespace sequencer::gateway::output::detail
