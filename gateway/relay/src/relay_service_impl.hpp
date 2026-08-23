#pragma once

// The client-facing Subscribe handshake — accepts the stream and hands
// it to RelayGatewayImpl::startSession, which does the actual work.
// Mirrors gateway/output/src/output_subscribe_service_impl.hpp's shape
// exactly; the two RPCs differ only in what happens after the
// handshake (a shared live Fanout there, an independent per-session
// tailing cursor here).

#include <brpc/closure_guard.h>
#include <brpc/controller.h>

#include "relay.pb.h"
#include "relay_gateway_impl.hpp"

namespace sequencer::gateway::relay::detail {

class RelayServiceImpl : public sequencer::gateway::relay::proto::RelayService {
 public:
  explicit RelayServiceImpl(RelayGatewayImpl& gateway) : gateway_(gateway) {}

  void Subscribe(::google::protobuf::RpcController* controllerBase,
                 const sequencer::gateway::relay::proto::RelaySubscribeRequest* request,
                 sequencer::gateway::relay::proto::RelaySubscribeResponse* response,
                 ::google::protobuf::Closure* done) override {
    brpc::ClosureGuard doneGuard(done);
    auto* cntl = static_cast<brpc::Controller*>(controllerBase);

    if (!cntl->has_remote_stream()) {
      response->set_error_message("Subscribe requires the client to attach a stream");
      return;
    }

    std::string errorMessage;
    if (!gateway_.startSession(*cntl, request->from_sequence_number(), &errorMessage)) {
      response->set_error_message(errorMessage);
    }
  }

 private:
  RelayGatewayImpl& gateway_;
};

}  // namespace sequencer::gateway::relay::detail
