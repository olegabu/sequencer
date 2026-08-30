#pragma once

// The client-facing Subscribe handshake: accepts the stream, registers
// it with the shared BrpcStreamFanout, and returns — publication happens
// later, entirely from the tailing thread calling codec->toOutput(),
// never from this handler.

#include <brpc/closure_guard.h>
#include <brpc/controller.h>
#include <brpc/stream.h>

#include "output_gateway.pb.h"
#include "brpc_stream_fanout.hpp"

namespace sequencer::gateway::output::detail {

class BrpcSubscribeServiceImpl : public sequencer::gateway::output::proto::OutputSubscribeService {
 public:
  explicit BrpcSubscribeServiceImpl(BrpcStreamFanout& fanout) : fanout_(fanout) {}

  void Subscribe(::google::protobuf::RpcController* controllerBase,
                 const sequencer::gateway::output::proto::OutputSubscribeRequest* request,
                 sequencer::gateway::output::proto::OutputSubscribeResponse* response,
                 ::google::protobuf::Closure* done) override {
    brpc::ClosureGuard doneGuard(done);
    auto* cntl = static_cast<brpc::Controller*>(controllerBase);

    if (!cntl->has_remote_stream()) {
      response->set_error_message("Subscribe requires the client to attach a stream");
      return;
    }

    brpc::StreamId streamId;
    brpc::StreamOptions options;
    options.handler = &fanout_;
    if (brpc::StreamAccept(&streamId, *cntl, &options) != 0) {
      response->set_error_message("StreamAccept failed");
      return;
    }

    const sequencer::SessionId sessionId =
        request->session_id() != 0 ? request->session_id() : fanout_.nextSessionId();
    const std::string topic = request->topic().empty() ? "all" : request->topic();
    fanout_.registerStream(streamId, sessionId, topic);
    response->set_session_id(sessionId);
  }

 private:
  BrpcStreamFanout& fanout_;
};

}  // namespace sequencer::gateway::output::detail
