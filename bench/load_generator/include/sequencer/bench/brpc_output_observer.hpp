#pragma once

// One of three output-gateway observers (see sequence_correlator.hpp's
// own file comment for the shared design) — this one subscribes to
// BrpcStreamTransport's OutputSubscribeService
// (gateway/output/proto/output_gateway.proto), the same brpc::Stream
// contract gateway/output/tests/collecting_stream_client.hpp's
// test-only CollectingStreamHandler already proves out (this mirrors
// its subscribe()/on_received_messages() shape, made production-grade:
// a real correlator behind it, not just a snapshot vector).
//
// No reader thread of its own, unlike the gRPC and WebSocket observers
// — brpc's on_received_messages() already fires asynchronously on
// brpc's own I/O thread pool, so there's nothing to poll: this class's
// only job is capturing the arrival instant and handing the payload to
// SequenceCorrelator::deliver(), directly from that callback.
//
// Safety note: this is the same StreamClose()/on_closed() gap
// documented at length elsewhere in this repository (gateway/output/
// README.md's "A real, rare crash this component's tests caught",
// gateway/relay/src/relay_session.hpp) — StreamClose() does not
// guarantee on_closed() has already run by the time it returns. stop()
// closes the stream and blocks, bounded, until on_closed() actually
// confirms, before this object is safe to destroy further.

#include <brpc/channel.h>
#include <brpc/controller.h>
#include <brpc/stream.h>

#include "output_gateway.pb.h"
#include "sequence_correlator.hpp"

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <stdexcept>
#include <string>

namespace sequencer::bench {

class BrpcOutputObserver final : public OutputGatewayObserver, private brpc::StreamInputHandler {
 public:
  // `outputGatewayAddr`: the brpc output gateway's listen address
  // ("host:port"). `topic`/`extractor`/`ringCapacityPow2`/
  // `correlatorThreads`: see GrpcOutputObserver's own constructor
  // comment — identical meaning.
  BrpcOutputObserver(std::string outputGatewayAddr, std::string topic,
                      SequenceCorrelator::SequenceExtractor extractor, std::size_t ringCapacityPow2,
                      int correlatorThreads = 4)
      : outputGatewayAddr_(std::move(outputGatewayAddr)),
        topic_(std::move(topic)),
        correlator_(std::move(extractor), ringCapacityPow2, correlatorThreads) {}

  ~BrpcOutputObserver() override { stop(); }

  BrpcOutputObserver(const BrpcOutputObserver&) = delete;
  BrpcOutputObserver& operator=(const BrpcOutputObserver&) = delete;

  // Call once, before the load generator starts sending — same
  // requirement as the other two observers. Throws on connection or
  // subscribe failure (matching collecting_stream_client.hpp's own
  // subscribe() convention) — a caller with something more graceful in
  // mind can catch it; a benchmark harness failing loudly and early is
  // the right default.
  void start() override {
    correlator_.start();
    brpc::ChannelOptions channelOptions;
    if (channel_.Init(outputGatewayAddr_.c_str(), &channelOptions) != 0) {
      throw std::runtime_error("BrpcOutputObserver: failed to connect to " + outputGatewayAddr_);
    }
    brpc::StreamOptions streamOptions;
    streamOptions.handler = this;
    if (brpc::StreamCreate(&streamId_, cntl_, &streamOptions) != 0) {
      throw std::runtime_error("BrpcOutputObserver: StreamCreate failed");
    }
    gateway::output::proto::OutputSubscribeService_Stub stub(&channel_);
    gateway::output::proto::OutputSubscribeRequest request;
    request.set_topic(topic_);
    gateway::output::proto::OutputSubscribeResponse response;
    stub.Subscribe(&cntl_, &request, &response, nullptr);
    if (cntl_.Failed() || !response.error_message().empty()) {
      throw std::runtime_error("BrpcOutputObserver: Subscribe failed: " + response.error_message());
    }
  }

  void setMeasurementWindow(std::int64_t measureStartUs, std::int64_t measureEndUs) override {
    correlator_.setMeasurementWindow(measureStartUs, measureEndUs);
  }

  // Idempotent, safe to omit (the destructor calls it too).
  void stop() override {
    if (streamId_ != brpc::INVALID_STREAM_ID) {
      brpc::StreamClose(streamId_);
      std::unique_lock<std::mutex> lock(closeMutex_);
      closedCv_.wait_for(lock, std::chrono::seconds(5), [this] { return closed_; });
      streamId_ = brpc::INVALID_STREAM_ID;
    }
    correlator_.stop();
  }

  void recordSend(std::uint64_t sequenceNumber, std::int64_t sendTimeUs) noexcept override {
    correlator_.recordSend(sequenceNumber, sendTimeUs);
  }

  void printSummary() const override {
    std::printf("\n=== output-gateway (brpc) observed summary ===\n");
    std::printf(
        "(submission to receipt via BrpcStreamTransport's own brpc::Stream -- not\n"
        " the synchronous ack path above; see bench/load_generator/README.md)\n");
    correlator_.printSummary("output_brpc");
  }

 private:
  static std::int64_t nowMicros() {
    return std::chrono::duration_cast<std::chrono::microseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
  }

  // brpc::StreamInputHandler: fires on brpc's own I/O thread pool, not
  // a thread this class owns — captures the arrival instant once per
  // callback invocation (every message in this batch arrived at
  // effectively the same instant from this observer's point of view,
  // same reasoning RelayObserver's own batch-read loop uses) and hands
  // each message straight to the correlator, doing nothing else here.
  int on_received_messages(brpc::StreamId, butil::IOBuf* const messages[], std::size_t size) override {
    const std::int64_t nowUs = nowMicros();
    for (std::size_t i = 0; i < size; ++i) {
      correlator_.deliver(messages[i]->to_string(), nowUs);
    }
    return 0;
  }
  void on_idle_timeout(brpc::StreamId) override {}
  void on_closed(brpc::StreamId) override {
    std::lock_guard<std::mutex> lock(closeMutex_);
    closed_ = true;
    closedCv_.notify_all();
  }

  std::string outputGatewayAddr_;
  std::string topic_;
  SequenceCorrelator correlator_;
  brpc::Channel channel_;
  brpc::Controller cntl_;
  brpc::StreamId streamId_ = brpc::INVALID_STREAM_ID;
  std::mutex closeMutex_;
  std::condition_variable closedCv_;
  bool closed_ = false;  // guarded by closeMutex_
};

}  // namespace sequencer::bench
