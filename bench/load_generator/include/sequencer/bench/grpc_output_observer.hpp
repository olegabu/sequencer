#pragma once

// One of three output-gateway observers (see sequence_correlator.hpp's
// own file comment for the shared design) — this one subscribes to
// GrpcOutputTransport's real-gRPC GenericOutputService
// (gateway/output/proto/output_grpc.proto), the output-side counterpart
// to RelayObserver's own gRPC subscription (relay_observer.hpp).
//
// This transport was originally one OutputRecord per Write(), same as
// the relay's own gRPC Subscribe before its batching fix — this
// observer's own benchmark numbers were what surfaced the bottleneck
// (~1.66s p50 at 70k msg/s, live fleet), and
// gateway/output/src/grpc_output_transport.cpp now batches for exactly
// the reasons documented in gateway/relay/README.md's "Batching the
// gRPC stream" section. Reads OutputRecordBatch here to match.

#include <grpcpp/grpcpp.h>

#include "output_grpc.grpc.pb.h"
#include "sequence_correlator.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>

namespace sequencer::bench {

class GrpcOutputObserver final : public OutputGatewayObserver {
 public:
  // `outputGatewayAddr`: the gRPC output gateway's listen address
  // ("host:port"). `topic`: matches output_grpc.proto's own "empty
  // means the catch-all every session joins by default" convention.
  // `extractor`/`ringCapacityPow2`/`correlatorThreads`: see
  // SequenceCorrelator's own constructor comment.
  GrpcOutputObserver(std::string outputGatewayAddr, std::string topic,
                      SequenceCorrelator::SequenceExtractor extractor, std::size_t ringCapacityPow2,
                      int correlatorThreads = 4)
      : outputGatewayAddr_(std::move(outputGatewayAddr)),
        topic_(std::move(topic)),
        correlator_(std::move(extractor), ringCapacityPow2, correlatorThreads) {}

  ~GrpcOutputObserver() override { stop(); }

  GrpcOutputObserver(const GrpcOutputObserver&) = delete;
  GrpcOutputObserver& operator=(const GrpcOutputObserver&) = delete;

  // Call once, before the load generator starts sending — a delivery
  // that races ahead of this call starting would otherwise be missed
  // (same requirement as RelayObserver::start()).
  void start() override {
    correlator_.start();
    auto channel = grpc::CreateChannel(outputGatewayAddr_, grpc::InsecureChannelCredentials());
    stub_ = gateway::output::grpc_proto::GenericOutputService::NewStub(channel);
    readerThread_ = std::thread([this] { readLoop(); });
  }

  void setMeasurementWindow(std::int64_t measureStartUs, std::int64_t measureEndUs) override {
    correlator_.setMeasurementWindow(measureStartUs, measureEndUs);
  }

  // Cancels the streaming call, joins the reader thread, then lets the
  // correlator pool drain — same ordering RelayObserver::stop() uses,
  // for the same reason (a queued item's arrival timestamp is already
  // captured; finishing it costs only wall-clock time, not accuracy).
  // Idempotent, safe to omit (the destructor calls it too).
  void stop() override {
    if (!stopRequested_.exchange(true, std::memory_order_relaxed)) {
      context_.TryCancel();
    }
    if (readerThread_.joinable()) {
      readerThread_.join();
    }
    correlator_.stop();
  }

  void recordSend(std::uint64_t sequenceNumber, std::int64_t sendTimeUs) noexcept override {
    correlator_.recordSend(sequenceNumber, sendTimeUs);
  }

  void printSummary() const override {
    std::printf("\n=== output-gateway (grpc) observed summary ===\n");
    std::printf(
        "(submission to receipt via GrpcOutputTransport's real gRPC stream -- not\n"
        " the synchronous ack path above; see bench/load_generator/README.md)\n");
    correlator_.printSummary("output_grpc");
  }

 private:
  static std::int64_t nowMicros() {
    return std::chrono::duration_cast<std::chrono::microseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
  }

  // Mirrors RelayObserver::readLoop()'s own shape exactly: capture the
  // arrival instant immediately after Read() returns, do nothing else
  // blocking here — the correlator's own pool handles extraction and
  // the (potentially blocking) wait.
  void readLoop() {
    gateway::output::grpc_proto::SubscribeRequest request;
    request.set_topic(topic_);
    std::unique_ptr<grpc::ClientReader<gateway::output::grpc_proto::OutputRecordBatch>> reader(
        stub_->Subscribe(&context_, request));

    gateway::output::grpc_proto::OutputRecordBatch batch;
    while (reader->Read(&batch)) {
      // Captured once per batch — every payload it carries arrived at
      // effectively the same instant from this observer's point of
      // view (see grpc_output_transport.cpp's own batching comment).
      const std::int64_t nowUs = nowMicros();
      for (const std::string& payload : batch.payloads()) {
        correlator_.deliver(payload, nowUs);
      }
    }
  }

  std::string outputGatewayAddr_;
  std::string topic_;
  SequenceCorrelator correlator_;
  std::atomic<bool> stopRequested_{false};
  std::thread readerThread_;
  std::unique_ptr<gateway::output::grpc_proto::GenericOutputService::Stub> stub_;
  grpc::ClientContext context_;
};

}  // namespace sequencer::bench
