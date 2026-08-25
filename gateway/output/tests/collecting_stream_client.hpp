#pragma once

// A test-only brpc::Stream client, shared by output_gateway_test.cpp
// and restart_drill_test.cpp.
//
// This exists specifically to close a gap symmetric to the one
// documented in gateway/output/README.md's "A real, rare crash this
// component's tests caught": brpc::StreamClose() does not guarantee
// its stream's on_closed() callback has already run by the time it
// returns — that fix was applied on the *server* side
// (StreamFanout::closeAll(), in src/stream_fanout.hpp). The exact same
// gap exists on the *client* side of every test that subscribes: if
// nothing waits for confirmed closure before a Subscription's
// unique_ptr<CollectingStreamHandler> is destroyed, an in-flight
// asynchronous on_closed()/on_received_messages() callback can land on
// already-freed memory. This was almost certainly the cause of a
// rare, one-off segfault caught in restart_drill_test.cpp (its extra
// teardown cycles — three gateway instances torn down in one test,
// versus at most two anywhere else — made the narrow timing window
// more likely to be hit, but the gap itself was equally present in
// output_gateway_test.cpp's original, separately-duplicated copy of
// this same handler).

#include <brpc/channel.h>
#include <brpc/controller.h>
#include <brpc/stream.h>

#include "output_gateway.pb.h"

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace sequencer::gateway::output::detail {

class CollectingStreamHandler : public brpc::StreamInputHandler {
 public:
  // Each of the `size` messages may itself be a batch of several
  // length-prefixed payloads now (StreamFanout::append()/flush(),
  // gateway/output/src/stream_fanout.hpp) — decoded and flattened
  // here so every test using this handler keeps seeing one already-
  // unwrapped payload per received_ entry, unchanged.
  int on_received_messages(brpc::StreamId, butil::IOBuf* const messages[], size_t size) override {
    std::lock_guard<std::mutex> lock(mutex_);
    for (size_t i = 0; i < size; ++i) {
      const std::string blob = messages[i]->to_string();
      size_t offset = 0;
      while (offset + 4 <= blob.size()) {
        const auto length = static_cast<uint32_t>(static_cast<unsigned char>(blob[offset]) << 24 |
                                                    static_cast<unsigned char>(blob[offset + 1]) << 16 |
                                                    static_cast<unsigned char>(blob[offset + 2]) << 8 |
                                                    static_cast<unsigned char>(blob[offset + 3]));
        offset += 4;
        if (offset + length > blob.size()) {
          break;  // truncated frame; shouldn't happen, drop rather than misparse
        }
        received_.push_back(blob.substr(offset, length));
        offset += length;
      }
    }
    return 0;
  }
  void on_idle_timeout(brpc::StreamId) override {}
  void on_closed(brpc::StreamId) override {
    std::lock_guard<std::mutex> lock(mutex_);
    closed_ = true;
    closedCv_.notify_all();
  }

  std::vector<std::string> snapshot() {
    std::lock_guard<std::mutex> lock(mutex_);
    return received_;
  }

  // Blocks, bounded, until on_closed() has actually fired. Call this
  // after brpc::StreamClose() and before this handler is destroyed —
  // see the file comment above for why skipping this is unsafe, not
  // just untidy.
  void waitForClose(std::chrono::seconds timeout = std::chrono::seconds(5)) {
    std::unique_lock<std::mutex> lock(mutex_);
    closedCv_.wait_for(lock, timeout, [this] { return closed_; });
  }

 private:
  std::mutex mutex_;
  std::condition_variable closedCv_;
  bool closed_ = false;
  std::vector<std::string> received_;
};

// RAII: closes the stream and waits for confirmed closure before
// letting `handler` be destroyed, in the destructor, so correctness
// never depends on every test remembering to do this by hand.
class Subscription {
 public:
  Subscription() = default;
  Subscription(std::unique_ptr<CollectingStreamHandler> handler, std::unique_ptr<brpc::Controller> cntl,
               brpc::StreamId streamId)
      : handler_(std::move(handler)), cntl_(std::move(cntl)), streamId_(streamId) {}

  Subscription(const Subscription&) = delete;
  Subscription& operator=(const Subscription&) = delete;

  ~Subscription() { close(); }

  void close() {
    if (streamId_ != brpc::INVALID_STREAM_ID) {
      brpc::StreamClose(streamId_);
      handler_->waitForClose();
      streamId_ = brpc::INVALID_STREAM_ID;
    }
  }

  CollectingStreamHandler& handler() { return *handler_; }

 private:
  std::unique_ptr<CollectingStreamHandler> handler_;
  std::unique_ptr<brpc::Controller> cntl_;
  brpc::StreamId streamId_ = brpc::INVALID_STREAM_ID;
};

inline Subscription subscribe(brpc::Channel& channel, const std::string& topic) {
  auto handler = std::make_unique<CollectingStreamHandler>();
  auto cntl = std::make_unique<brpc::Controller>();
  brpc::StreamOptions options;
  options.handler = handler.get();
  brpc::StreamId streamId = brpc::INVALID_STREAM_ID;
  if (brpc::StreamCreate(&streamId, *cntl, &options) != 0) {
    throw std::runtime_error("StreamCreate failed");
  }

  sequencer::gateway::output::proto::OutputSubscribeService_Stub stub(&channel);
  sequencer::gateway::output::proto::OutputSubscribeRequest request;
  request.set_topic(topic);
  sequencer::gateway::output::proto::OutputSubscribeResponse response;
  stub.Subscribe(cntl.get(), &request, &response, nullptr);
  if (cntl->Failed() || !response.error_message().empty()) {
    throw std::runtime_error("Subscribe failed: " + response.error_message());
  }
  return Subscription(std::move(handler), std::move(cntl), streamId);
}

inline bool waitForCount(CollectingStreamHandler& handler, std::size_t count, std::chrono::seconds timeout) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    if (handler.snapshot().size() >= count) {
      return true;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  return false;
}

}  // namespace sequencer::gateway::output::detail
