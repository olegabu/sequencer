#pragma once

// The generic chassis loop of specification.md §8.6, concretely: this
// is `RunInputGateway`'s body. Every line here is generic except the
// two calls into `codec_` — exactly the split §8.6 argues for.
//
// The proposal is issued ASYNCHRONOUSLY and this handler returns
// immediately; the response is finished from the proposer's callback.
// That matters because a gateway makes exactly one proposal per
// client request, so a handler that waits for the node in place holds
// a brpc worker for a full cross-AZ round trip and the gateway's
// capacity becomes (workers / RTT) — nothing to do with the raft group
// behind it. Measured: at 100k req/s the raft group answers in ~727us
// when a client calls it directly, while the same load through a
// blocking gateway measured ~1320us, and merely raising the gateway's
// worker count to 256 moved that to ~891us. Async removes the ceiling
// rather than raising it. See raft-tests/sequencer/README.md's "What
// the input gateway costs".

#include <brpc/closure_guard.h>
#include <brpc/controller.h>
#include <butil/iobuf.h>

#include <cerrno>
#include <span>
#include <string>
#include <vector>

#include <sequencer/input_codec.hpp>
#include <sequencer/signature_verifier.hpp>

#include "input_gateway.pb.h"
#include "node_proposer.hpp"

namespace sequencer::gateway::input::detail {

class SubmitServiceImpl : public sequencer::gateway::input::proto::SubmitService {
 public:
  // `shape` defaults to RequestResponse: that is what every transport
  // this chassis serves today is (brpc baidu_std, brpc's gRPC, HTTP
  // +JSON), and it keeps existing callers unchanged. A SessionStream
  // transport must pass it explicitly -- see TransportShape.
  SubmitServiceImpl(InputCodec& codec, NodeProposer& proposer, SignatureVerifier verifier,
                     TransportShape shape = TransportShape::RequestResponse)
      : codec_(codec), proposer_(proposer), verifier_(std::move(verifier)), shape_(shape) {}

  void Submit(::google::protobuf::RpcController* controllerBase,
              const sequencer::gateway::input::proto::SubmitRequest* /*request*/,
              sequencer::gateway::input::proto::SubmitResponse* /*response*/,
              ::google::protobuf::Closure* done) override {
    // Guards only the early-return paths below; once the proposal is
    // handed off it is released, and the callback owns `done`.
    brpc::ClosureGuard doneGuard(done);
    auto* cntl = static_cast<brpc::Controller*>(controllerBase);

    // acceptClientRequest() (generic) — the raw body, regardless of
    // which protocol (HTTP, baidu_std, gRPC) delivered it; see
    // input_gateway.proto's comment for why an empty request schema
    // makes this uniform.
    const std::string body = cntl->request_attachment().to_string();
    const ClientRequest request{Payload(reinterpret_cast<const std::byte*>(body.data()), body.size())};

    // codec->toInput(request) — the one line that needs application
    // knowledge.
    const Result<Bytes> inputResult = codec_.toInput(request);
    if (!inputResult.ok()) {
      cntl->SetFailed(EINVAL, "%s", inputResult.error().c_str());
      return;
    }
    const Bytes& inputBytes = inputResult.value();
    const Payload input(inputBytes.data(), inputBytes.size());

    // verifyClientSignature(input) (generic).
    if (!verifier_(input)) {
      cntl->SetFailed(EINVAL, "invalid client signature");
      return;
    }

    // proposeToLeader(input) (generic) — asynchronous, so this
    // handler's worker is free the moment the RPC is on the wire.
    doneGuard.release();
    proposer_.proposeAsync(input, [this, cntl, done](NodeProposer::Outcome outcome) {
      // Runs on a brpc callback thread once the node answers; taking
      // the guard here is what actually completes the client's call.
      brpc::ClosureGuard callbackGuard(done);
      if (!outcome.ok) {
        cntl->SetFailed(EIO, "%s", outcome.errorMessage.c_str());
        return;
      }

      // codec->toOutput(receipt, designatedOutputs) — application
      // knowledge again.
      //
      // specification.md §8.11: which path delivers an output is fixed
      // by the transport's SHAPE. On a RequestResponse transport (this
      // one, and every transport the chassis serves today) the
      // designated outputs are the reply. On a SessionStream transport
      // they must NOT be delivered here at all -- the output side
      // sends every output for that session from the journal, in
      // sequence-number order -- so the chassis hands the codec an
      // empty span rather than trusting each transport to remember.
      // Enforced here, not by convention, so a future FIX input
      // transport cannot double-deliver by omission.
      std::vector<Payload> designatedSpans;
      if (shape_ == TransportShape::RequestResponse) {
        designatedSpans.reserve(outcome.designatedOutputs.size());
        for (const Bytes& output : outcome.designatedOutputs) {
          designatedSpans.emplace_back(output.data(), output.size());
        }
      }
      const Bytes responseBytes = codec_.toOutput(
          outcome.receipt, std::span<const Payload>(designatedSpans.data(), designatedSpans.size()));

      // sendClientResponse(response) (generic).
      cntl->response_attachment().append(responseBytes.data(), responseBytes.size());
    });
  }

 private:
  InputCodec& codec_;
  NodeProposer& proposer_;
  SignatureVerifier verifier_;
  const TransportShape shape_ = TransportShape::RequestResponse;
};

}  // namespace sequencer::gateway::input::detail
