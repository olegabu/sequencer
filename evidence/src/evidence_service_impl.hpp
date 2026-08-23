#pragma once

// specification.md §7.2: "signed roots are themselves published... an
// inclusion proof is reconstructible from public data forever" — this
// is the RPC surface that does the publishing/serving. Purely a
// read-only front for SigningGatewayImpl's in-memory (bounds, root,
// signature) map; all the actual block-cutting and signing happens on
// the tailing thread, never here.

#include <brpc/closure_guard.h>
#include <brpc/controller.h>

#include "evidence.pb.h"
#include "signing_gateway_impl.hpp"

namespace sequencer::evidence::detail {

class EvidenceServiceImpl : public sequencer::evidence::proto::EvidenceService {
 public:
  explicit EvidenceServiceImpl(const SigningGatewayImpl& gateway) : gateway_(gateway) {}

  void GetSignedRoot(::google::protobuf::RpcController* /*controllerBase*/,
                      const sequencer::evidence::proto::GetSignedRootRequest* request,
                      sequencer::evidence::proto::GetSignedRootResponse* response,
                      ::google::protobuf::Closure* done) override {
    brpc::ClosureGuard doneGuard(done);

    const std::optional<SignedBlockMeta> meta = gateway_.signedBlock(request->block_index());
    if (!meta.has_value()) {
      response->set_found(false);
      return;
    }
    response->set_found(true);
    response->set_first_sequence_number(meta->firstSequenceNumber);
    response->set_last_sequence_number(meta->lastSequenceNumber);
    response->set_root(meta->root.data(), meta->root.size());
    response->set_signature(meta->signature.data(), meta->signature.size());
  }

  void GetInclusionProof(::google::protobuf::RpcController* /*controllerBase*/,
                          const sequencer::evidence::proto::GetInclusionProofRequest* request,
                          sequencer::evidence::proto::GetInclusionProofResponse* response,
                          ::google::protobuf::Closure* done) override {
    brpc::ClosureGuard doneGuard(done);

    const std::optional<InclusionProof> proof = gateway_.inclusionProof(request->sequence_number());
    if (!proof.has_value()) {
      response->set_found(false);
      return;
    }
    response->set_found(true);
    response->set_first_sequence_number(proof->blockFirstSequenceNumber);
    response->set_last_sequence_number(proof->blockLastSequenceNumber);
    response->set_root(proof->root.data(), proof->root.size());
    response->set_signature(proof->signature.data(), proof->signature.size());
    for (const Hash32& sibling : proof->siblingHashes) {
      response->add_sibling_hashes(sibling.data(), sibling.size());
    }
  }

 private:
  const SigningGatewayImpl& gateway_;
};

}  // namespace sequencer::evidence::detail
