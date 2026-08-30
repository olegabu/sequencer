// BrpcOutputTransport's implementation — the brpc Streaming RPC
// transport (specification.md §8.7).
//
// Everything here was once OutputGatewayImpl's hardcoded transport;
// it became a transport behind the OutputTransport interface so that a
// different one (gRPC, WebSocket) could be swapped in without touching
// the tailing loop. It lives in a .cpp, matching the other two
// transports, so that the public header names no brpc type.

#include <sequencer/brpc_output_transport.hpp>

#include <brpc/server.h>

#include <stdexcept>
#include <string>

#include "brpc_stream_fanout.hpp"
#include "brpc_subscribe_service_impl.hpp"

namespace sequencer {

struct BrpcOutputTransport::Impl {
  Impl() : subscribeService(fanout) {}

  gateway::output::detail::BrpcStreamFanout fanout;
  gateway::output::detail::BrpcSubscribeServiceImpl subscribeService;
  brpc::Server server;
};

BrpcOutputTransport::BrpcOutputTransport() : impl_(std::make_unique<Impl>()) {}

BrpcOutputTransport::~BrpcOutputTransport() = default;

void BrpcOutputTransport::attach(BroadcastRing& ring, TopicRegistry& topics,
                                  int idleSpinIterations) {
  impl_->fanout.attach(ring, topics, idleSpinIterations);
}

void BrpcOutputTransport::start(int listenPort) {
  if (impl_->server.AddService(&impl_->subscribeService, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
    throw std::runtime_error(
        "BrpcOutputTransport::start: AddService(OutputSubscribeService) failed");
  }
  brpc::ServerOptions serverOptions;
  if (impl_->server.Start(listenPort, &serverOptions) != 0) {
    throw std::runtime_error("BrpcOutputTransport::start: brpc::Server::Start failed on port " +
                              std::to_string(listenPort));
  }
}

void BrpcOutputTransport::stop() {
  // See BrpcStreamFanout::closeAll()'s comment: this must happen, and
  // must actually confirm every stream closed, before the server (and
  // this object) can safely be torn down.
  impl_->fanout.closeAll();
  impl_->server.Stop(0);
  impl_->server.Join();
}

}  // namespace sequencer
