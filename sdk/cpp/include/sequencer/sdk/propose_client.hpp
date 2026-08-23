#pragma once

// specification.md §9: sdk/'s "propose" responsibility — but with a
// transport-agnostic design, because the dependency arrows in the same
// section keep sdk/ off of both gateway/ and node/ ("sdk depends on
// journal and evidence's proof format," full stop). Concretely, this
// means ProposeClient cannot itself know how to speak to an input
// gateway (application-specific protocol) or a node's ProposeService
// (a node/-owned proto this library isn't allowed to depend on) — so
// the actual send is a caller-supplied callback, and this class
// contributes only the two things that are always the same regardless
// of transport: signing a payload exactly once via client_signer.hpp,
// and specification.md §8.1's "on timeout resubmit blindly" retry loop
// that resends those identical signed bytes, never regenerating them,
// so consecutive attempts share one idempotency key by construction.
//
// See sdk/cpp/tests/propose_client_test.cpp for a concrete instance:
// wiring the transport callback to a real node's ProposeService,
// following redirects, against a real single-node NodeImpl.

#include <functional>
#include <optional>
#include <string>
#include <utility>

#include <sequencer/payload.hpp>
#include <sequencer/sdk/client_signer.hpp>

namespace sequencer::sdk {

struct ProposeOutcome {
  bool ok = false;
  Receipt receipt{};
  Bytes designatedOutput;
  std::string errorMessage;
};

// Sends `requestBytes` (already signed, if a signing key was
// configured) to wherever the application's input path lives, and
// returns the outcome of that one attempt. `errorMessage` on a failed
// attempt is purely diagnostic — ProposeClient does not parse or act
// on it beyond surfacing it if every attempt fails.
using ProposeTransport = std::function<ProposeOutcome(Payload requestBytes)>;

class ProposeClient {
 public:
  explicit ProposeClient(ProposeTransport transport) : transport_(std::move(transport)) {}

  // specification.md §7: sign every payload with this client's key
  // before it's ever sent, so the signature is present in the bytes
  // the transport submits (and therefore in whatever gets journaled).
  // Not set in the constructor because a pass-through deployment might
  // reasonably run without client-side signing at all (the input
  // gateway's SignatureVerifier hook still defaults to
  // acceptAllSignatures — see gateway/input/README.md).
  void setSigningKey(Ed25519PrivateKey key) { signingKey_ = key; }

  // specification.md §8.1: "on timeout resubmit blindly (state-machine
  // deduplication absorbs duplicates)." `payload` is signed exactly
  // once; every retry resends the identical resulting bytes.
  ProposeOutcome propose(Payload payload, int maxAttempts = 3) {
    const Bytes requestBytes =
        signingKey_.has_value() ? signPayload(payload, *signingKey_) : Bytes(payload.begin(), payload.end());
    const Payload requestView(requestBytes.data(), requestBytes.size());

    ProposeOutcome outcome;
    for (int attempt = 0; attempt < maxAttempts; ++attempt) {
      outcome = transport_(requestView);
      if (outcome.ok) {
        return outcome;
      }
    }
    return outcome;
  }

 private:
  ProposeTransport transport_;
  std::optional<Ed25519PrivateKey> signingKey_;
};

}  // namespace sequencer::sdk
