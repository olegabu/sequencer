#include <sequencer/grpc_output_transport.hpp>

#include "output_batch_metrics.hpp"

#include <sequencer/output_codec.hpp>

#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>

#include "output_grpc.grpc.pb.h"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <string>
#include <vector>

// gRPC's synchronous server-streaming API runs each Subscribe() call
// on its own dedicated thread for the call's entire lifetime. Under
// the previous push-based design that thread spent its life draining
// a mutex/condvar queue the tailing thread fed — the hand-off whose
// queueing delay the BroadcastRing redesign exists to remove (see
// include/sequencer/broadcast_ring.hpp's file comment). Under the
// ring design that same dedicated thread IS the subscriber's reader:
// it drains the ring through its own private cursor and calls the
// blocking Write() itself, so the sync API's thread-per-call shape —
// previously the liability that motivated (and sank: measured no
// better, see examples/counter/README.md) a reactor rewrite — is now
// exactly the per-subscriber thread the design wants anyway.
//
// Batched on the wire, as before: each Write() carries an
// OutputRecordBatch of everything the reader drained this pass —
// never delaying a send to wait for more, so a caught-up session
// still gets exactly one payload per batch.

namespace sequencer {

namespace {

// Shared attach-time state plus the couple of registry bits Subscribe
// needs — split out from GrpcOutputTransport::Impl so the service
// class can be fully defined before Impl (which owns one).
struct SubscribeShared {
  BroadcastRing* ring = nullptr;
  TopicRegistry* topics = nullptr;
  int idleSpinIterations = 1000;
  std::atomic<SessionId> nextSessionId{1};
  std::atomic<bool> stopping{false};
};

class GenericOutputServiceImpl final : public gateway::output::grpc_proto::GenericOutputService::Service {
 public:
  explicit GenericOutputServiceImpl(SubscribeShared& shared) : shared_(shared) {}

  ::grpc::Status Subscribe(::grpc::ServerContext* context,
                            const gateway::output::grpc_proto::SubscribeRequest* request,
                            ::grpc::ServerWriter<gateway::output::grpc_proto::OutputRecordBatch>* writer) override {
    const SessionId sessionId = shared_.nextSessionId.fetch_add(1, std::memory_order_relaxed);
    const std::uint64_t topicTag = makeTopicTag(shared_.topics->idFor(request->topic()));
    const std::uint64_t sessionTag = makeSessionTag(sessionId);

    std::uint64_t cursor = shared_.ring->head();  // live-only, from the moment of subscription
    std::vector<std::byte> payload(shared_.ring->maxPayload());
    IdleStrategy idle(shared_.idleSpinIterations);

    // ~1024 payloads per batch cap, matching the relay's own
    // FLAGS_relay_max_batch_records default and reasoning — comfortably
    // under gRPC's 4MB message cap for counter-sized payloads. The
    // idle strategy's back-off sleep bounds how stale the IsCancelled
    // check can get while a caught-up session waits.
    constexpr int kMaxBatch = 1024;
    while (!context->IsCancelled() && !shared_.stopping.load(std::memory_order_relaxed)) {
      gateway::output::grpc_proto::OutputRecordBatch batch;
      bool overrun = false;
      while (batch.payloads_size() < kMaxBatch) {
        std::uint64_t tag = 0;
        std::uint32_t length = 0;
        const auto result = shared_.ring->readOne(cursor, tag, payload.data(), length);
        if (result == BroadcastRing::ReadResult::Empty) {
          break;
        }
        if (result == BroadcastRing::ReadResult::Overrun) {
          overrun = true;
          break;
        }
        if (tag != topicTag && tag != sessionTag) {
          continue;  // someone else's entry; not counted against the batch cap
        }
        batch.add_payloads(payload.data(), length);
      }
      if (batch.payloads_size() > 0) {
        if (!writer->Write(batch)) {
          break;  // client gone
        }
        gateway::output::detail::grpcBatchMetrics().recordBatch(batch.payloads_size());
        idle.reset();
      }
      if (overrun) {
        // Lapped by the producer — this subscriber is genuinely not
        // keeping up. Disconnect rather than silently skip
        // (broadcast_ring.hpp's slow-consumer contract).
        return ::grpc::Status(::grpc::StatusCode::DATA_LOSS,
                               "subscriber overrun: fell more than a full ring behind");
      }
      if (batch.payloads_size() == 0) {
        idle.idle();
      }
    }
    return ::grpc::Status::OK;
  }

 private:
  SubscribeShared& shared_;
};

}  // namespace

struct GrpcOutputTransport::Impl {
  SubscribeShared shared;
  GenericOutputServiceImpl service{shared};
  std::unique_ptr<grpc::Server> server;
};

GrpcOutputTransport::GrpcOutputTransport() : impl_(std::make_unique<Impl>()) {}
GrpcOutputTransport::~GrpcOutputTransport() = default;

void GrpcOutputTransport::attach(BroadcastRing& ring, TopicRegistry& topics, int idleSpinIterations) {
  impl_->shared.ring = &ring;
  impl_->shared.topics = &topics;
  impl_->shared.idleSpinIterations = idleSpinIterations;
}

void GrpcOutputTransport::start(int listenPort) {
  grpc::reflection::InitProtoReflectionServerBuilderPlugin();
  grpc::ServerBuilder builder;
  builder.AddListeningPort("0.0.0.0:" + std::to_string(listenPort), grpc::InsecureServerCredentials());
  builder.RegisterService(&impl_->service);
  impl_->server = builder.BuildAndStart();
}

void GrpcOutputTransport::stop() {
  impl_->shared.stopping.store(true, std::memory_order_relaxed);
  if (impl_->server) {
    // The deadline cancels in-flight Subscribe calls that don't
    // notice `stopping` on their own first (each notices within one
    // idle back-off at worst); Wait() then confirms every handler
    // thread has actually returned before this object can be torn
    // down.
    impl_->server->Shutdown(std::chrono::system_clock::now() + std::chrono::milliseconds(500));
    impl_->server->Wait();
  }
}

}  // namespace sequencer
