#pragma once

// The chassis's built-in InputTransport: brpc, serving baidu_std and
// brpc's own gRPC natively plus HTTP with an arbitrary body
// (specification.md §8.7's zero-additional-dependency choice; see
// input_gateway.proto for how one empty request schema covers all
// three).
//
// This is the FIRST implementation of InputTransport, not a special
// case beside it. The chassis loop it used to contain now lives in
// request_pipeline.hpp, shared with every other transport; what
// remains here is genuinely brpc-specific -- a Server, a service, and
// the Controller/Closure pair that answers one request.
//
// The proposal is issued asynchronously and the handler returns at
// once; the response is finished from the proposer's callback. That
// matters because a gateway makes exactly one proposal per client
// request, so a handler that waits in place holds a brpc worker for a
// full cross-AZ round trip and the gateway's capacity becomes
// (workers / RTT) -- nothing to do with the raft group behind it.
// Measured: at 100k req/s the raft group answers in ~727us called
// directly, while the same load through a blocking gateway measured
// ~1320us, and merely raising the worker count to 256 moved that to
// ~891us. Async removes the ceiling rather than raising it. See
// raft-tests/sequencer/README.md's "What the input gateway costs".

#include <brpc/closure_guard.h>
#include <brpc/controller.h>
#include <brpc/server.h>
#include <butil/iobuf.h>

#include <cerrno>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>

#include <sequencer/input_transport.hpp>

#include "input_gateway.pb.h"

namespace sequencer::gateway::input::detail {

// One in-flight brpc request. Holds the Controller and Closure alive
// until the chassis answers, which happens on a proposer callback
// thread long after the handler returned.
class BrpcRequestContext : public sequencer::RequestContext {
 public:
  BrpcRequestContext(brpc::Controller* controller, ::google::protobuf::Closure* done)
      : controller_(controller), done_(done) {
    body_ = controller_->request_attachment().to_string();
  }

  sequencer::Payload body() const override {
    return sequencer::Payload(reinterpret_cast<const std::byte*>(body_.data()), body_.size());
  }

  void respond(sequencer::Payload response) override {
    brpc::ClosureGuard guard(done_);
    controller_->response_attachment().append(response.data(), response.size());
  }

  void fail(const std::string& message) override {
    brpc::ClosureGuard guard(done_);
    controller_->SetFailed(EINVAL, "%s", message.c_str());
  }

 private:
  brpc::Controller* controller_;
  ::google::protobuf::Closure* done_;
  // Copied out of the attachment once: the chassis reads it after the
  // handler returns, and an IOBuf's storage is not guaranteed beyond
  // that.
  std::string body_;
};

class BrpcSubmitService : public sequencer::gateway::input::proto::SubmitService {
 public:
  explicit BrpcSubmitService(sequencer::InputTransport::RequestFn onRequest)
      : onRequest_(std::move(onRequest)) {}

  void Submit(::google::protobuf::RpcController* controllerBase,
              const sequencer::gateway::input::proto::SubmitRequest* /*request*/,
              sequencer::gateway::input::proto::SubmitResponse* /*response*/,
              ::google::protobuf::Closure* done) override {
    auto* controller = static_cast<brpc::Controller*>(controllerBase);
    // Ownership of `done` passes to the context: it is released by
    // whichever of respond()/fail() the chassis calls, on whatever
    // thread that happens on.
    onRequest_(std::make_shared<BrpcRequestContext>(controller, done));
  }

 private:
  sequencer::InputTransport::RequestFn onRequest_;
};

class BrpcInputTransport : public sequencer::InputTransport {
 public:
  // brpc is request/response in every protocol it serves here, so this
  // is a constant rather than a setting (§8.11).
  sequencer::TransportShape shape() const override {
    return sequencer::TransportShape::RequestResponse;
  }

  void attach(RequestFn onRequest, DisconnectFn /*onDisconnect*/) override {
    // No disconnect callback: these are sessionless protocols, so there
    // is no session loss for InputCodec::onDisconnect to hear about.
    service_ = std::make_unique<BrpcSubmitService>(std::move(onRequest));
  }

  void start(int listenPort) override {
    if (service_ == nullptr) {
      throw std::runtime_error("BrpcInputTransport::start: attach() was not called");
    }
    if (server_.AddService(service_.get(), brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
      throw std::runtime_error("BrpcInputTransport::start: AddService(SubmitService) failed");
    }
    brpc::ServerOptions serverOptions;
    if (server_.Start(listenPort, &serverOptions) != 0) {
      throw std::runtime_error("BrpcInputTransport::start: brpc::Server::Start failed on port " +
                                std::to_string(listenPort));
    }
  }

  void stop() override {
    server_.Stop(0);
    server_.Join();
  }

 private:
  std::unique_ptr<BrpcSubmitService> service_;
  brpc::Server server_;
};

}  // namespace sequencer::gateway::input::detail
