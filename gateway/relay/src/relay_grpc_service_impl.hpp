#pragma once

// The real-gRPC counterpart to relay_service_impl.hpp — see
// proto/relay_grpc.proto's file comment for why this is a separate
// service/codegen unit rather than an extension of the brpc::Stream-
// based one.
//
// Unlike RelaySession (relay_session.hpp), this needs no separate
// session object or async callback machinery at all: gRPC's
// synchronous server-streaming API runs each Subscribe() call on its
// own dedicated thread for the call's entire lifetime, so the tailing
// loop is simply written directly in this method, blocking on
// grpc::ServerWriter::Write() the same way RelaySession's tailing
// thread blocks on brpc::StreamWrite() — just without needing a
// thread of its own to be spawned, since gRPC already provides one per
// call.
//
// Batched, not one record per Write(): see gateway/relay/README.md's
// "Batching the gRPC stream" section for the live-fleet numbers behind
// this. A synchronous Write() call's own fixed per-call overhead
// dominated at counter-record sizes (~40-50 bytes on the wire), so
// each iteration now gathers every record already available (up to
// FLAGS_relay_max_batch_records) into one RecordBatch before writing —
// never delaying a send to wait for more to arrive. Caught up, this
// degrades to exactly the old one-record-per-Write behavior (nothing
// else is available yet); behind, it collapses however large the
// backlog is into far fewer Write() round trips.

#include <gflags/gflags.h>
#include <grpcpp/grpcpp.h>

#include "relay_gateway_impl.hpp"
#include "relay_grpc.grpc.pb.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <thread>

// ~1024 x ~50 bytes ≈ 50KB per batch at counter-record sizes, comfortably
// under gRPC's 4MB default message-size cap; tune per deployment if
// records are much larger. Included in exactly one translation unit per
// binary (run_relay_gateway.cpp, relay_grpc_test.cpp — separate
// executables), so defining the flag directly in this header is safe.
DEFINE_int32(relay_max_batch_records, 1024,
             "Max journal records the relay's gRPC Subscribe stream gathers into one "
             "RecordBatch/Write() call when a backlog exists. Never delays a send to "
             "accumulate a batch — caught up, every batch is exactly 1 record.");

namespace sequencer::gateway::relay::detail {

class RelayGrpcServiceImpl final : public grpc_proto::RelayService::Service {
 public:
  explicit RelayGrpcServiceImpl(RelayGatewayImpl& gateway) : gateway_(gateway) {}

  // Called right before grpc::Server::Shutdown(): an idle subscriber's
  // Subscribe() call below has nothing else to wake it — there's no
  // more data coming and nothing else ever sets IsCancelled() — so
  // without this, shutdown would stall for the caller's full Shutdown()
  // deadline. Checked every poll (already every 5ms), so this makes
  // shutdown near-instant instead; Shutdown()'s own deadline becomes
  // just a backstop, the same "belt and suspenders" relationship
  // grpc_output_transport.cpp's closeAll() has with its own Shutdown().
  void requestStop() { stopRequested_.store(true, std::memory_order_relaxed); }

  ::grpc::Status Subscribe(::grpc::ServerContext* context, const grpc_proto::SubscribeRequest* request,
                            ::grpc::ServerWriter<grpc_proto::RecordBatch>* writer) override {
    const std::shared_ptr<journal::JournalReader> reader = gateway_.waitForReader(std::chrono::seconds(5));
    if (!reader) {
      return ::grpc::Status(::grpc::StatusCode::UNAVAILABLE, "journal not yet available");
    }

    const int maxBatch = std::max(1, FLAGS_relay_max_batch_records);
    std::uint64_t nextSeq = request->from_sequence_number() == 0 ? 1 : request->from_sequence_number();
    while (!context->IsCancelled() && !stopRequested_.load(std::memory_order_relaxed)) {
      if (!reader->contains(nextSeq)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        continue;
      }
      grpc_proto::RecordBatch batch;
      // Deliberately NOT re-checking context->IsCancelled() on every
      // iteration here: unlike stopRequested_ (a relaxed atomic load,
      // essentially free), IsCancelled() plucks gRPC's own completion
      // queue under the hood — cheap once per batch (the outer loop
      // above), catastrophic per record (measured: collapses backlog
      // catch-up throughput by roughly two orders of magnitude, since
      // it dominates every single gather iteration instead of the
      // actual record read). A batch can run at most one iteration
      // past a client that disconnects mid-gather — bounded by
      // maxBatch either way, and the outer loop's own check catches it
      // on the next batch.
      for (int gathered = 0; gathered < maxBatch && reader->contains(nextSeq) &&
                              !stopRequested_.load(std::memory_order_relaxed);
           ++gathered, ++nextSeq) {
        const journal::RecordView view = reader->record(nextSeq);
        const Payload raw = view.rawBytes();
        batch.add_raw_records(raw.data(), raw.size());
      }
      if (batch.raw_records_size() == 0) {
        continue;  // cancelled/stopped mid-gather, nothing to send
      }
      if (!writer->Write(batch)) {
        break;  // client gone
      }
    }
    return ::grpc::Status::OK;
  }

 private:
  RelayGatewayImpl& gateway_;
  std::atomic<bool> stopRequested_{false};
};

}  // namespace sequencer::gateway::relay::detail
