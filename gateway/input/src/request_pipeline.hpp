#pragma once

// The input gateway's chassis loop (specification.md §8.6), independent
// of any transport.
//
// This is the whole of what §8.6's pseudocode describes --
// acceptClientRequest, codec->toInput, verifyClientSignature,
// proposeToLeader, codec->toOutput, sendClientResponse -- with the
// first and last delegated to an InputTransport. It used to live inside
// the brpc service class, which meant a second transport would have had
// to reimplement it or inherit from a brpc type; moving it here is what
// makes "one chassis, several transports" true rather than aspirational.
//
// specification.md §8.11's rule is enforced here, at the one place
// every transport passes through.

#include <memory>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include <sequencer/input_codec.hpp>
#include <sequencer/input_transport.hpp>
#include <sequencer/payload.hpp>
#include <sequencer/signature_verifier.hpp>

#include "node_proposer.hpp"

namespace sequencer::gateway::input::detail {

class RequestPipeline {
 public:
  RequestPipeline(sequencer::InputCodec& codec, NodeProposer& proposer,
                   sequencer::SignatureVerifier verifier, sequencer::TransportShape shape,
                   bool inlineDesignatedOnSession = false)
      : codec_(codec),
        proposer_(proposer),
        verifier_(std::move(verifier)),
        shape_(shape),
        inlineDesignatedOnSession_(inlineDesignatedOnSession) {}

  RequestPipeline(const RequestPipeline&) = delete;
  RequestPipeline& operator=(const RequestPipeline&) = delete;

  void handle(std::shared_ptr<sequencer::RequestContext> request) {
    // acceptClientRequest() (generic) — the raw body, whatever protocol
    // delivered it.
    const sequencer::ClientRequest clientRequest{request->body(), request->session()};

    // codec->toInput(request) — the one line that needs application
    // knowledge.
    const sequencer::Result<sequencer::Bytes> inputResult = codec_.toInput(clientRequest);
    if (!inputResult.ok()) {
      request->fail(inputResult.error());
      return;
    }
    const sequencer::Bytes& inputBytes = inputResult.value();
    const sequencer::Payload input(inputBytes.data(), inputBytes.size());

    // verifyClientSignature(input) (generic).
    if (!verifier_(input)) {
      request->fail("invalid client signature");
      return;
    }

    // proposeToLeader(input) (generic) — asynchronous, so the calling
    // thread is free the moment the RPC is on the wire.
    // Only the inline path needs the input bytes to survive into the
    // callback, and copying them per request is not free -- so the copy
    // is made only when that path is on. Empty otherwise.
    sequencer::Bytes inputForReply =
        inlineDesignatedOnSession_ ? inputBytes : sequencer::Bytes();

    proposer_.proposeAsync(input, [this, request, inputForReply = std::move(inputForReply)](
                                       NodeProposer::Outcome outcome) {
      if (!outcome.ok) {
        request->fail(outcome.errorMessage);
        return;
      }

      // specification.md §8.11: which path delivers an output is fixed
      // by the transport's SHAPE. On a RequestResponse transport the
      // designated outputs are the reply. On a SessionStream transport
      // they must NOT be delivered here -- the output side sends every
      // output for that session from the journal, in sequence-number
      // order -- so the codec is handed an empty span rather than each
      // transport being trusted to remember. A future FIX transport
      // cannot double-deliver by omission.
      //
      // inlineDesignatedOnSession_ is the deliberate, flagged exception
      // (see InputGatewayConfig). It lets a SessionStream transport
      // answer from the receipt, and makes the transport responsible for
      // suppressing the journal copy of the same output. It is NOT the
      // default, and it is only sound for sessions whose every output
      // originates in their own inputs -- see the flag's comment.
      std::vector<sequencer::Payload> designated;
      if (shape_ == sequencer::TransportShape::RequestResponse || inlineDesignatedOnSession_) {
        designated.reserve(outcome.designatedOutputs.size());
        for (const sequencer::Bytes& output : outcome.designatedOutputs) {
          designated.emplace_back(output.data(), output.size());
        }
      }

      // codec->toOutput(receipt, designatedOutputs) — application
      // knowledge again.
      const sequencer::Bytes responseBytes = codec_.toOutput(
          outcome.receipt,
          std::span<const sequencer::Payload>(designated.data(), designated.size()),
          sequencer::Payload(inputForReply.data(), inputForReply.size()));

      // The journal position this input landed at, so a transport that
      // answers inline can tell the output side exactly which record and
      // how many outputs it has already sent. sequenceNumber is on the
      // receipt already -- no application-level execution id is needed.
      request->noteReceipt(outcome.receipt, outcome.designatedOutputs.size());

      // sendClientResponse(response) (generic).
      request->respond(sequencer::Payload(responseBytes.data(), responseBytes.size()));
    });
  }

  // specification.md §8.1: "propose a disconnect input on session loss,
  // if the state machine defines one". A codec returning nullopt means
  // the state machine has no notion of one, and nothing is proposed.
  void handleDisconnect(const sequencer::SessionInfo& session) {
    const std::optional<sequencer::Bytes> input = codec_.onDisconnect(session);
    if (!input.has_value()) {
      return;
    }
    // Fire and forget: there is no client left to answer.
    auto owned = std::make_shared<sequencer::Bytes>(*input);
    proposer_.proposeAsync(sequencer::Payload(owned->data(), owned->size()),
                            [owned](NodeProposer::Outcome) {});
  }

 private:
  sequencer::InputCodec& codec_;
  NodeProposer& proposer_;
  sequencer::SignatureVerifier verifier_;
  const sequencer::TransportShape shape_;
  const bool inlineDesignatedOnSession_ = false;
};

}  // namespace sequencer::gateway::input::detail
