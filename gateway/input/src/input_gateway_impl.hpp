#pragma once

// Ties together a client-facing InputTransport, the NodeProposer, and
// the codec — everything RunInputGateway (input_gateway.hpp) sets up,
// minus argv/gflags parsing, exactly as node/'s NodeImpl is separated
// from RunNode, so this class can be driven directly and
// deterministically from tests.
//
// The transport is pluggable (specification.md §8.10 choice (b)): brpc
// by default, a FIX session gateway or anything else by passing a
// factory. The chassis loop itself is in request_pipeline.hpp and is
// the same whichever transport is in use.

#include <functional>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <sequencer/input_codec.hpp>
#include <sequencer/input_transport.hpp>
#include <sequencer/signature_verifier.hpp>

#include "brpc_input_transport.hpp"
#include "node_proposer.hpp"
#include "request_pipeline.hpp"

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
  // Superseded by the transport's own shape() when a transport is
  // supplied; retained because a deployment may still want to force
  // SessionStream semantics onto a request/response transport for
  // testing the guard.
  sequencer::TransportShape transportShape = sequencer::TransportShape::RequestResponse;

  // Answer a SessionStream transport's client from the propose receipt
  // instead of waiting for the output side to deliver the same output
  // from the journal, saving that hop's latency.
  //
  // OFF by default, because it trades away a property §8.12 depends on.
  // With it on, a session's own replies leave ~200us before outputs
  // that the journal ordered around them, so the order a client was
  // sent messages in is no longer the order the journal reproduces --
  // and the journal IS the resend store, so a ResendRequest would
  // replay a different sequence than was originally transmitted.
  //
  // It is sound only where every output a session receives originates
  // in that session's own inputs (order entry with no market data and
  // no fills caused by other clients). There, propose order and journal
  // order coincide and nothing can overtake anything.
  //
  // The transport must suppress the journal copy of what it sent
  // inline; noteReceipt() gives it the journal position to do that.
  bool inlineDesignatedOnSession = false;
};

// Builds the transport a gateway will serve. Mirrors
// RunOutputGateway's transport factory (specification.md §8.5).
using InputTransportFactory = std::function<std::unique_ptr<sequencer::InputTransport>()>;

inline InputTransportFactory defaultInputTransportFactory() {
  return [] { return std::unique_ptr<sequencer::InputTransport>(std::make_unique<BrpcInputTransport>()); };
}

class InputGatewayImpl {
 public:
  InputGatewayImpl(InputGatewayConfig config, std::unique_ptr<sequencer::InputCodec> codec,
                    sequencer::SignatureVerifier verifier = sequencer::acceptAllSignatures,
                    InputTransportFactory transportFactory = defaultInputTransportFactory())
      : config_(std::move(config)),
        codec_(std::move(codec)),
        proposer_(config_.nodeEndpoints, config_.maxBatchSize, config_.maxInFlightBatches),
        transport_(transportFactory()),
        // The TRANSPORT's shape wins over the config's: §8.11 makes the
        // shape a property of the transport, not a deployment choice,
        // and a transport that declares SessionStream must not be able
        // to have request/response delivery configured back on. The
        // config value only applies to the default transport, which
        // declares RequestResponse anyway -- so it is reachable in
        // tests without weakening the rule.
        pipeline_(*codec_, proposer_, std::move(verifier),
                   transport_->shape() == sequencer::TransportShape::SessionStream
                       ? sequencer::TransportShape::SessionStream
                       : config_.transportShape,
                   config_.inlineDesignatedOnSession) {}

  InputGatewayImpl(const InputGatewayImpl&) = delete;
  InputGatewayImpl& operator=(const InputGatewayImpl&) = delete;

  void start() {
    transport_->attach(
        [this](std::shared_ptr<sequencer::RequestContext> request) {
          pipeline_.handle(std::move(request));
        },
        [this](const sequencer::SessionInfo& session) { pipeline_.handleDisconnect(session); });
    transport_->start(config_.listenPort);
  }

  void stop() { transport_->stop(); }

  int listenPort() const { return config_.listenPort; }

 private:
  InputGatewayConfig config_;
  std::unique_ptr<sequencer::InputCodec> codec_;
  NodeProposer proposer_;
  std::unique_ptr<sequencer::InputTransport> transport_;
  RequestPipeline pipeline_;
};

}  // namespace sequencer::gateway::input::detail
