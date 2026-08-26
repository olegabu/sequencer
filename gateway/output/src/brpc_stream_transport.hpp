#pragma once

// The chassis's built-in OutputTransport: brpc's own Streaming RPC
// (specification.md §8.7). Everything here already existed as
// OutputGatewayImpl's hardcoded transport; this is that same StreamFanout
// + OutputSubscribeServiceImpl + brpc::Server, now behind the
// OutputTransport interface so a different transport (e.g. WebSocket)
// can be swapped in without touching the tailing loop.

#include <brpc/server.h>

#include <memory>
#include <stdexcept>
#include <string>

#include <sequencer/output_transport.hpp>

#include "output_subscribe_service_impl.hpp"
#include "stream_fanout.hpp"

namespace sequencer::gateway::output::detail {

class BrpcStreamTransport : public sequencer::OutputTransport {
 public:
  BrpcStreamTransport() : subscribeService_(fanout_) {}

  void attach(sequencer::BroadcastRing& ring, sequencer::TopicRegistry& topics,
              int idleSpinIterations) override {
    fanout_.attach(ring, topics, idleSpinIterations);
  }

  void start(int listenPort) override {
    if (server_.AddService(&subscribeService_, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
      throw std::runtime_error("BrpcStreamTransport::start: AddService(OutputSubscribeService) failed");
    }
    brpc::ServerOptions serverOptions;
    if (server_.Start(listenPort, &serverOptions) != 0) {
      throw std::runtime_error("BrpcStreamTransport::start: brpc::Server::Start failed on port " +
                                std::to_string(listenPort));
    }
  }

  void stop() override {
    // See StreamFanout::closeAll()'s comment: this must happen, and
    // must actually confirm every stream closed, before the server
    // (and this object) can safely be torn down.
    fanout_.closeAll();
    server_.Stop(0);
    server_.Join();
  }

 private:
  StreamFanout fanout_;
  OutputSubscribeServiceImpl subscribeService_;
  brpc::Server server_;
};

}  // namespace sequencer::gateway::output::detail
