#pragma once

// Wires EvidenceServiceImpl to a real brpc::Server — split out from
// SigningGatewayImpl (tailing/signing/querying only) the same way
// gateway/output split BrpcStreamTransport out of OutputGatewayImpl:
// keeps the tailing logic's own header free of brpc::Server plumbing,
// and lets tests stand up just the tailing/signing half without a
// listening port when they don't need one.

#include <stdexcept>

#include <brpc/server.h>

#include "evidence_service_impl.hpp"
#include "signing_gateway_impl.hpp"

namespace sequencer::evidence::detail {

class EvidenceServer {
 public:
  EvidenceServer(const SigningGatewayImpl& gateway, int listenPort) : service_(gateway) {
    if (server_.AddService(&service_, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
      throw std::runtime_error("EvidenceServer: AddService failed");
    }
    brpc::ServerOptions options;
    if (server_.Start(listenPort, &options) != 0) {
      throw std::runtime_error("EvidenceServer: Start failed on port " + std::to_string(listenPort));
    }
  }

  ~EvidenceServer() { stop(); }

  EvidenceServer(const EvidenceServer&) = delete;
  EvidenceServer& operator=(const EvidenceServer&) = delete;

  void stop() {
    server_.Stop(0);
    server_.Join();
  }

 private:
  EvidenceServiceImpl service_;
  brpc::Server server_;
};

}  // namespace sequencer::evidence::detail
