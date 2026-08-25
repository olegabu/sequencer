#pragma once

// The concrete, built-in Fanout: delivers to clients connected via
// OutputSubscribeService over brpc's own Streaming RPC (specification.md
// §8.7 — the zero-additional-dependency choice for brpc/gRPC-aware
// consumers). A different Fanout (e.g. WebSocket, via Boost.Beast) can
// be plugged in elsewhere without touching OutputCodec or the tailing
// loop — this is just the one this chassis ships with.

#include <brpc/stream.h>
#include <butil/iobuf.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
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
    write(streamId, bytes);
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
      write(streamId, bytes);
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
  void write(brpc::StreamId streamId, const Bytes& bytes) {
    butil::IOBuf buf;
    buf.append(bytes.data(), bytes.size());
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
  std::unordered_set<brpc::StreamId> pendingClose_;
  std::atomic<sequencer::SessionId> nextSessionId_{1};
};

}  // namespace sequencer::gateway::output::detail
