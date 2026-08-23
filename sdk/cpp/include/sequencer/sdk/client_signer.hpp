#pragma once

// specification.md §7: "every input carries the submitting client's
// signature over its exact wire bytes, persisted inside the journaled
// input" — but "no concrete signature scheme is specified anywhere in
// the spec; choosing and shipping one is sdk/'s job"
// (gateway/input/README.md). This is that scheme: a fixed envelope,
// `signature(64 bytes) ‖ payload`, signed with the same raw-Ed25519
// primitive evidence/'s Merkle roots are signed with
// (sequencer::evidence::signBytes) — reused directly rather than
// reimplemented, so there is exactly one OpenSSL-Ed25519 code path in
// this repository, not two that could quietly drift apart.
//
// Deliberately depends on nothing beyond evidence/ and journal/'s
// Payload/Bytes (specification.md §9's dependency arrows: "sdk depends
// on journal and evidence's proof format" — not on gateway/input). The
// returned verifier is a plain `std::function<bool(Payload)>`, which is
// exactly gateway/input's `SignatureVerifier` type — an application
// wires the two together itself, at zero extra dependency, since it's
// the same underlying type alias.

#include <cstddef>
#include <functional>
#include <stdexcept>

#include <sequencer/evidence/merkle.hpp>
#include <sequencer/payload.hpp>

namespace sequencer::sdk {

using evidence::Ed25519PrivateKey;
using evidence::Ed25519PublicKey;
using evidence::Signature64;

inline constexpr std::size_t kSignatureEnvelopeOverhead = std::tuple_size<Signature64>::value;

// Wraps `payload` as `signature ‖ payload` — the bytes an InputCodec
// should return from toInput() when the application wants every input
// signed with this scheme, and exactly the bytes the chassis's
// SignatureVerifier hook (and later, evidence/'s Merkle leaves) will
// see, since specification.md §7 requires the signature to be
// persisted inside the journaled input itself, not stripped before
// proposing.
inline Bytes signPayload(Payload payload, const Ed25519PrivateKey& privateKey) {
  const Signature64 signature = evidence::signBytes(privateKey, payload);
  Bytes envelope;
  envelope.reserve(signature.size() + payload.size());
  envelope.insert(envelope.end(), signature.begin(), signature.end());
  envelope.insert(envelope.end(), payload.begin(), payload.end());
  return envelope;
}

// The payload portion of an envelope produced by signPayload — what a
// StateMachine or InputCodec that has adopted this scheme should
// interpret, once the chassis's SignatureVerifier has already checked
// it (specification.md §8.1: verification happens once, at the input
// gateway; nothing downstream needs to re-parse the signature).
inline Payload envelopePayload(Payload input) {
  if (input.size() < kSignatureEnvelopeOverhead) {
    throw std::invalid_argument("envelopePayload: input shorter than the signature it must carry");
  }
  return input.subspan(kSignatureEnvelopeOverhead);
}

inline bool verifyEnvelopeSignature(Payload input, const Ed25519PublicKey& publicKey) {
  if (input.size() < kSignatureEnvelopeOverhead) {
    return false;
  }
  Signature64 signature{};
  for (std::size_t i = 0; i < signature.size(); ++i) {
    signature[i] = input[i];
  }
  return evidence::verifyBytesSignature(publicKey, envelopePayload(input), signature);
}

// A ready-to-plug verifier: `RunInputGateway`'s chassis invokes
// whatever `SignatureVerifier` (`std::function<bool(Payload)>`) an
// application supplies on every proposed input (see
// gateway/input/include/sequencer/signature_verifier.hpp, whose default
// `acceptAllSignatures` is an explicit non-verifying placeholder until
// an application wires up something real — this is that something
// real).
inline std::function<bool(Payload)> makeEnvelopeSignatureVerifier(Ed25519PublicKey publicKey) {
  return [publicKey](Payload input) { return verifyEnvelopeSignature(input, publicKey); };
}

}  // namespace sequencer::sdk
