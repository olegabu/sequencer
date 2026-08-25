#pragma once

// The concrete, built-in Fanout: delivers to clients connected via
// OutputSubscribeService over brpc's own Streaming RPC (specification.md
// §8.7 — the zero-additional-dependency choice for brpc/gRPC-aware
// consumers). A different Fanout (e.g. WebSocket, via Boost.Beast) can
// be plugged in elsewhere without touching OutputCodec or the tailing
// loop — this is just the one this chassis ships with.
//
// append()/flush() batch multiple toSession()/broadcast() payloads
// into one brpc::StreamWrite() call (see flush()'s own comment for
// why) — but brpc::Stream is message-oriented, not a raw byte stream:
// on_received_messages() delivers whatever one StreamWrite() call
// sent as one opaque unit, so simply concatenating several payloads'
// raw bytes into one buffer would produce one corrupted, unparseable
// "message" on the receiving end instead of several correct ones.
// Each append() therefore writes its own 4-byte big-endian length
// prefix ahead of the payload — the minimal framing that lets a batch
// of N payloads still decode as N payloads, deliberately not a
// protobuf envelope (unlike GrpcOutputTransport's own
// OutputRecordBatch): this transport's whole point, per
// specification.md §8.7 and this file's own header, is carrying an
// OutputCodec's bytes completely unmodified, no envelope at all, for
// consumers with no protobuf dependency of their own — the length
// prefix is the smallest addition that preserves that while still
// allowing a batch. gateway/output/include/sequencer/bench (this
// repo's own benchmark observer) and gateway/output/tests/
// collecting_stream_client.hpp both decode it; any other brpc client
// of this transport needs to as well now.

#include <brpc/stream.h>
#include <butil/iobuf.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#include <sequencer/output_codec.hpp>

namespace sequencer::gateway::output::detail {

class StreamFanout : public sequencer::Fanout, public brpc::StreamInputHandler {
 public:
  sequencer::SessionId nextSessionId() { return nextSessionId_.fetch_add(1, std::memory_order_relaxed); }

  // Registers a newly-accepted stream under `sessionId`, joined to
  // `topic`. Called by OutputSubscribeServiceImpl right after
  // brpc::StreamAccept.
  void registerStream(brpc::StreamId streamId, sequencer::SessionId sessionId, const std::string& topic) {
    std::lock_guard<std::mutex> lock(mutex_);
    sessionToStream_[sessionId] = streamId;
    streamToSession_[streamId] = sessionId;
    streamToTopic_[streamId] = topic;
    topicToStreams_[topic].insert(streamId);
  }

  // Force-closes every currently-registered stream and waits for each
  // one's on_closed() to actually fire before returning.
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
  // object." Called by OutputGatewayImpl::stop() before stopping the
  // server — and, transitively, before anything owning this fanout can
  // be destroyed.
  void closeAll() {
    std::unique_lock<std::mutex> lock(mutex_);
    std::vector<brpc::StreamId> streams;
    streams.reserve(streamToSession_.size());
    for (const auto& [streamId, sessionId] : streamToSession_) {
      (void)sessionId;
      streams.push_back(streamId);
    }
    pendingClose_.insert(streams.begin(), streams.end());
    lock.unlock();

    for (brpc::StreamId streamId : streams) {
      brpc::StreamClose(streamId);
    }

    lock.lock();
    closedCv_.wait_for(lock, std::chrono::seconds(5), [this] { return pendingClose_.empty(); });
  }

  void toSession(sequencer::SessionId owner, Bytes bytes) override {
    brpc::StreamId streamId = brpc::INVALID_STREAM_ID;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      const auto it = sessionToStream_.find(owner);
      if (it == sessionToStream_.end()) {
        return;  // session not currently connected; a fanout delivery is best-effort
      }
      streamId = it->second;
    }
    append(streamId, bytes);
  }

  void broadcast(const std::string& topic, Bytes bytes) override {
    std::vector<brpc::StreamId> targets;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      const auto it = topicToStreams_.find(topic);
      if (it == topicToStreams_.end()) {
        return;
      }
      targets.assign(it->second.begin(), it->second.end());
    }
    for (brpc::StreamId streamId : targets) {
      append(streamId, bytes);
    }
  }

  // Sends everything accumulated by append() since the last flush(),
  // one brpc::StreamWrite() per stream that actually has pending data
  // — not one per toSession()/broadcast() call. Live-fleet perf
  // profiling of a caller doing the old one-StreamWrite()-per-record
  // thing found ~100k/sec of individual write()/sendmsg() syscalls
  // (plus real kernel spinlock contention on top) was the dominant
  // cost of the whole output-gateway pipeline: measured p50 ~4.7ms at
  // 100k msg/s despite this gateway's own tailing loop itself
  // accounting for barely 1% of CPU self-time in that same profile —
  // almost all of it was TCP/socket-write path, not application code.
  // See gateway/output/src/output_gateway_impl.hpp's tailLoop() for
  // where flush() actually gets called (once per gathered batch, not
  // once per record) and OutputGatewayImpl::stop() for why a graceful
  // shutdown calls it once more unconditionally.
  void flush() override {
    std::unordered_map<brpc::StreamId, butil::IOBuf> toSend;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (pending_.empty()) {
        return;
      }
      toSend.swap(pending_);
    }
    for (auto& [streamId, buf] : toSend) {
      write(streamId, buf);
    }
  }

  // brpc::StreamInputHandler — this fanout only ever writes; clients
  // never send anything over their subscribe stream.
  int on_received_messages(brpc::StreamId, butil::IOBuf* const*, size_t) override { return 0; }
  void on_idle_timeout(brpc::StreamId) override {}
  void on_closed(brpc::StreamId streamId) override { deregister(streamId); }

 private:
  // Bounded retry on EAGAIN — the remote hasn't drained enough of what
  // was already sent, expected backpressure under real throughput, not
  // a failure. A single-shot attempt (this method's own earlier
  // version) silently and permanently dropped the message whenever
  // that happened, which turned out to mean *almost every* message
  // under real load: a benchmark subscriber consuming this same
  // broadcast at 70k records/sec received nearly none of them before
  // this fix (bench/load_generator/'s output-gateway observers,
  // examples/counter/README.md's own "four round trips" section).
  // Short, not RelaySession::writeRecord()'s up-to-5-second bound
  // (gateway/relay/src/relay_session.hpp) — deliberately: this call
  // runs on the one tailing thread this gateway's every subscriber
  // shares (broadcast() loops over all of them for each record), so a
  // long retry here head-of-line-blocks every other subscriber behind
  // one slow one, not just the slow one itself. ~10ms is enough to
  // absorb ordinary momentary backpressure without meaningfully
  // stalling the others; a subscriber still EAGAIN after that is
  // genuinely not keeping up, and dropping (not blocking longer) is
  // the right tradeoff for a shared fanout.
  // Appends this payload's length-prefixed frame to the stream's
  // pending buffer under mutex_ — cheap (IOBuf::append is a
  // reference-counted block append, not a copy of existing content),
  // called once per toSession()/broadcast() call, same as write() used
  // to be. The actual send happens in flush(); see this file's own top
  // comment for why a length prefix, not a raw concatenation.
  void append(brpc::StreamId streamId, const Bytes& bytes) {
    std::uint8_t lengthPrefix[4];
    const auto length = static_cast<std::uint32_t>(bytes.size());
    lengthPrefix[0] = static_cast<std::uint8_t>(length >> 24);
    lengthPrefix[1] = static_cast<std::uint8_t>(length >> 16);
    lengthPrefix[2] = static_cast<std::uint8_t>(length >> 8);
    lengthPrefix[3] = static_cast<std::uint8_t>(length);

    std::lock_guard<std::mutex> lock(mutex_);
    butil::IOBuf& buf = pending_[streamId];
    buf.append(lengthPrefix, sizeof(lengthPrefix));
    buf.append(bytes.data(), bytes.size());
  }

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

  void deregister(brpc::StreamId streamId) {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto sessionIt = streamToSession_.find(streamId);
    if (sessionIt != streamToSession_.end()) {
      sessionToStream_.erase(sessionIt->second);
      streamToSession_.erase(sessionIt);
    }
    const auto topicIt = streamToTopic_.find(streamId);
    if (topicIt != streamToTopic_.end()) {
      auto setIt = topicToStreams_.find(topicIt->second);
      if (setIt != topicToStreams_.end()) {
        setIt->second.erase(streamId);
        if (setIt->second.empty()) {
          topicToStreams_.erase(setIt);
        }
      }
      streamToTopic_.erase(topicIt);
    }
    // A disconnected stream's own unflushed bytes (if flush() hasn't
    // run since its last append()) can never be sent — drop them
    // rather than let them accumulate in pending_ forever.
    pending_.erase(streamId);
    if (pendingClose_.erase(streamId) > 0 && pendingClose_.empty()) {
      closedCv_.notify_all();
    }
  }

  std::mutex mutex_;
  std::condition_variable closedCv_;
  std::unordered_map<sequencer::SessionId, brpc::StreamId> sessionToStream_;
  std::unordered_map<brpc::StreamId, sequencer::SessionId> streamToSession_;
  std::unordered_map<brpc::StreamId, std::string> streamToTopic_;
  std::unordered_map<std::string, std::unordered_set<brpc::StreamId>> topicToStreams_;
  std::unordered_map<brpc::StreamId, butil::IOBuf> pending_;  // accumulated by append(), sent by flush()
  std::unordered_set<brpc::StreamId> pendingClose_;
  std::atomic<sequencer::SessionId> nextSessionId_{1};
};

}  // namespace sequencer::gateway::output::detail
