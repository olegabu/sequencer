#pragma once

// specification.md §7, §8.1: an input gateway MUST verify the client
// signature embedded in every input before proposing it. No concrete
// signature scheme is specified anywhere in the spec — choosing and
// shipping one is sdk/'s job (a later phase, per specification.md §9's
// "the reference client library... verify proofs"). This header is
// deliberately just the hook: a callable the chassis invokes on every
// input, defaulting to an explicit, clearly-labeled placeholder that
// accepts everything, so the rest of the chassis is usable — and
// testable — before a real verifier exists to plug in.

#include <functional>

#include <sequencer/payload.hpp>

namespace sequencer {

using SignatureVerifier = std::function<bool(Payload input)>;

// THE DEFAULT IS NOT A REAL VERIFIER. It accepts every input
// unconditionally. Do not deploy this default anywhere a client
// signature must actually be checked (specification.md §7's whole
// evidence chain assumes it is) — pass a real SignatureVerifier once
// sdk/ has one.
inline bool acceptAllSignatures(Payload /*input*/) { return true; }

}  // namespace sequencer
