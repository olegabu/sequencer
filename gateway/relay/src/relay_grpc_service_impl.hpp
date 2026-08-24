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

#include <grpcpp/grpcpp.h>

#include "relay_gateway_impl.hpp"
#include "relay_grpc.grpc.pb.h"

#include <atomic>
#include <chrono>
#include <thread>

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
                            ::grpc::ServerWriter<grpc_proto::Record>* writer) override {
    const std::shared_ptr<journal::JournalReader> reader = gateway_.waitForReader(std::chrono::seconds(5));
    if (!reader) {
      return ::grpc::Status(::grpc::StatusCode::UNAVAILABLE, "journal not yet available");
    }

    std::uint64_t nextSeq = request->from_sequence_number() == 0 ? 1 : request->from_sequence_number();
    while (!context->IsCancelled() && !stopRequested_.load(std::memory_order_relaxed)) {
      if (!reader->contains(nextSeq)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        continue;
      }
      const journal::RecordView view = reader->record(nextSeq);
      const Payload raw = view.rawBytes();
      grpc_proto::Record record;
      record.set_raw_record(raw.data(), raw.size());
      if (!writer->Write(record)) {
        break;  // client gone
      }
      ++nextSeq;
    }
    return ::grpc::Status::OK;
  }

 private:
  RelayGatewayImpl& gateway_;
  std::atomic<bool> stopRequested_{false};
};

}  // namespace sequencer::gateway::relay::detail
