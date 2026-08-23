#pragma once

// Ties together the client-facing brpc server, the NodeProposer, and
// the codec — everything RunInputGateway (input_gateway.hpp) sets up,
// minus argv/gflags parsing, exactly as node/'s NodeImpl is separated
// from RunNode, so this class can be driven directly and
// deterministically from tests.

#include <brpc/server.h>

#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include <sequencer/input_codec.hpp>
#include <sequencer/signature_verifier.hpp>

#include "node_proposer.hpp"
#include "submit_service_impl.hpp"

namespace sequencer::gateway::input::detail {

struct InputGatewayConfig {
  std::vector<std::string> nodeEndpoints;  // "ip:port" — the raft group's Propose endpoints
  int listenPort = 0;
};

class InputGatewayImpl {
 public:
  InputGatewayImpl(InputGatewayConfig config, std::unique_ptr<sequencer::InputCodec> codec,
                    sequencer::SignatureVerifier verifier = sequencer::acceptAllSignatures)
      : config_(std::move(config)),
        codec_(std::move(codec)),
        proposer_(config_.nodeEndpoints),
        service_(*codec_, proposer_, std::move(verifier)) {}

  InputGatewayImpl(const InputGatewayImpl&) = delete;
  InputGatewayImpl& operator=(const InputGatewayImpl&) = delete;

  void start() {
    if (server_.AddService(&service_, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
      throw std::runtime_error("InputGatewayImpl::start: AddService(SubmitService) failed");
    }
    brpc::ServerOptions serverOptions;
    if (server_.Start(config_.listenPort, &serverOptions) != 0) {
      throw std::runtime_error("InputGatewayImpl::start: brpc::Server::Start failed on port " +
                                std::to_string(config_.listenPort));
    }
  }

  void stop() {
    server_.Stop(0);
    server_.Join();
  }

  int listenPort() const { return config_.listenPort; }

 private:
  InputGatewayConfig config_;
  std::unique_ptr<sequencer::InputCodec> codec_;
  NodeProposer proposer_;
  SubmitServiceImpl service_;
  brpc::Server server_;
};

}  // namespace sequencer::gateway::input::detail
