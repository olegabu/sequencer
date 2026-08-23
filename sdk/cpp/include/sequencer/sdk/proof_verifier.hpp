#pragma once

// specification.md §9.1: "the verification-only slice of sdk/ —
// hashing a record, walking sibling hashes, checking a signature — is
// header-only... a client embedding just proof verification should not
// have to link the full propose-and-reconcile machinery to get it."
// This is that slice: a thin, header-only wrapper around
// evidence::verifyInclusionProof that does the one thing a caller of
// that function must otherwise get right by hand — reconstructing
// rawRecordBytes exactly as journal::JournalWriter would have encoded
// it, via journal::encodeRecord (the same function the journal itself
// uses), rather than the caller improvising its own byte layout.
//
// specification.md §7.4: "verify a proof against locally retained
// submitted bytes, never against anything the venue echoes back" — the
// input and outputs passed here must be the caller's own record of
// what it submitted and independently knows this record contains, not
// anything read from the proof or handed back by the operator.

#include <span>
#include <vector>

#include <sequencer/evidence/merkle.hpp>
#include <sequencer/journal/record_view.hpp>
#include <sequencer/payload.hpp>

namespace sequencer::sdk {

using evidence::InclusionProof;

inline bool verifyInclusionProof(const InclusionProof& proof, const evidence::Ed25519PublicKey& publicKey,
                                  Payload input, std::span<const Payload> outputs) {
  Bytes rawRecordBytes(journal::recordEncodedSize(input, outputs));
  journal::encodeRecord(rawRecordBytes.data(), proof.sequenceNumber, input, outputs);
  return evidence::verifyInclusionProof(proof, publicKey, Payload(rawRecordBytes.data(), rawRecordBytes.size()));
}

// Convenience overload for the common single-output case
// (specification.md §5.2's designated output) — most state machines,
// examples/counter's included, journal at most one output per record.
inline bool verifyInclusionProof(const InclusionProof& proof, const evidence::Ed25519PublicKey& publicKey,
                                  Payload input, Payload output) {
  const std::vector<Payload> outputs = {output};
  return verifyInclusionProof(proof, publicKey, input, std::span<const Payload>(outputs));
}

}  // namespace sequencer::sdk
