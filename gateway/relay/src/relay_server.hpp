#pragma once

// Wires RelayServiceImpl to a real brpc::Server — split out from
// RelayGatewayImpl (journal tailing/session management only) the same
// way evidence/src/evidence_server.hpp splits EvidenceServer out of
// SigningGatewayImpl: keeps the tailing logic's own header free of
// brpc::Server plumbing, and lets tests stand up just that half
// without a listening port when they don't need one.

#include <stdexcept>
#include <string>

#include <brpc/server.h>

#include "relay_gateway_impl.hpp"
#include "relay_service_impl.hpp"

namespace sequencer::gateway::relay::detail {

class RelayServer {
 public:
  RelayServer(RelayGatewayImpl& gateway, int listenPort) : service_(gateway) {
    if (server_.AddService(&service_, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
      throw std::runtime_error("RelayServer: AddService failed");
    }
    brpc::ServerOptions options;
    if (server_.Start(listenPort, &options) != 0) {
      throw std::runtime_error("RelayServer: Start failed on port " + std::to_string(listenPort));
    }
  }

  ~RelayServer() { stop(); }

  RelayServer(const RelayServer&) = delete;
  RelayServer& operator=(const RelayServer&) = delete;

  void stop() {
    server_.Stop(0);
    server_.Join();
  }

 private:
  RelayServiceImpl service_;
  brpc::Server server_;
};

}  // namespace sequencer::gateway::relay::detail
