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
  // Proposal batching bounds; 0 keeps NodeProposer's own defaults.
  // See node_proposer.hpp's proposeAsync comment for why both exist
  // and what happened when neither did.
  std::size_t maxBatchSize = 0;
  int maxInFlightBatches = 0;
  // specification.md §8.11: which path delivers an output to a client
  // is fixed by the transport's shape, not chosen per message. Every
  // transport this chassis serves today is RequestResponse; a session
  // transport (FIX) sets SessionStream, and the chassis then withholds
  // designated outputs from the codec entirely, because the output
  // side delivers them from the journal instead.
  sequencer::TransportShape transportShape = sequencer::TransportShape::RequestResponse;
};

class InputGatewayImpl {
 public:
  InputGatewayImpl(InputGatewayConfig config, std::unique_ptr<sequencer::InputCodec> codec,
                    sequencer::SignatureVerifier verifier = sequencer::acceptAllSignatures)
      : config_(std::move(config)),
        codec_(std::move(codec)),
        proposer_(config_.nodeEndpoints, config_.maxBatchSize, config_.maxInFlightBatches),
        service_(*codec_, proposer_, std::move(verifier), config_.transportShape) {}

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
