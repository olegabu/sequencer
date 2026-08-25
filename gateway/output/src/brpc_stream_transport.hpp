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

  void toSession(sequencer::SessionId owner, Bytes bytes) override { fanout_.toSession(owner, std::move(bytes)); }
  void broadcast(const std::string& topic, Bytes bytes) override { fanout_.broadcast(topic, std::move(bytes)); }
  // Must forward explicitly: Fanout::flush()'s own default is a no-op,
  // and this class doesn't inherit from StreamFanout — without this
  // override, OutputGatewayImpl::tailLoop()'s transport_->flush() call
  // (through the OutputTransport/Fanout base pointer it actually holds)
  // never reaches StreamFanout::flush() at all, so nothing StreamFanout
  // accumulates via append() ever actually gets sent.
  void flush() override { fanout_.flush(); }

 private:
  StreamFanout fanout_;
  OutputSubscribeServiceImpl subscribeService_;
  brpc::Server server_;
};

}  // namespace sequencer::gateway::output::detail
