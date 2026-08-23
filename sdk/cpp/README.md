# sdk/cpp/

The C++ reference client library — see [sdk/README.md](../README.md)
for the per-language framing and the transport-agnostic design decision
this is built around. Entirely header-only: every piece here needs only
`journal/` (header-only) and `evidence/` (header-only) plus OpenSSL, so
linking this library costs nothing beyond what those two already cost.

## The pieces

- **`client_signer.hpp`** — specification.md §7's client-signature
  scheme: a fixed envelope, `signature(64 bytes) ‖ payload`, signed with
  the exact same raw-Ed25519 primitive (`evidence::signBytes`)
  `evidence/`'s Merkle roots are signed with — reused directly, not
  reimplemented, so there is exactly one OpenSSL-Ed25519 code path in
  this repository. `makeEnvelopeSignatureVerifier(publicKey)` returns a
  plain `std::function<bool(Payload)>` — structurally
  `gateway/input`'s `SignatureVerifier` type without `sdk/` actually
  depending on `gateway/input`'s header, since it's the same underlying
  type alias.
- **`proof_verifier.hpp`** — the header-only verification slice
  specification.md §9.1 calls out by name. A thin wrapper around
  `evidence::verifyInclusionProof` that reconstructs `rawRecordBytes`
  via `journal::encodeRecord` (the same function the journal itself
  uses) instead of leaving the caller to get that byte layout right by
  hand.
- **`alarm.hpp`** — `ProofTimeoutAlarm`: specification.md §7.4's "treat
  a proof's non-arrival within a bounded interval as a first-class
  alarm," as a minimal register/clear/query tracker. No crypto, no
  journal — just `<chrono>` and a map.
- **`propose_client.hpp`** — `ProposeClient`: signs a payload once (if a
  signing key is configured) and resubmits the identical resulting
  bytes on timeout (specification.md §8.1), through a caller-supplied
  transport callback. See sdk/README.md for why the transport is
  pluggable rather than built in.
- **`reconciler.hpp`** — `Reconciler`: specification.md §7.4's "reconcile
  acknowledgements against the published journal routinely — an O(1)
  check per record," via a colocated `journal::JournalReader` (the same
  "colocated consumers may memory-map the journal file directly"
  allowance §3 grants everywhere else in this repository). Compares a
  caller-retained `(sequenceNumber, rawRecordBytes)` pair against what's
  actually published — never the reverse.

## Testing

```sh
cmake --preset debug
cmake --build --preset debug
ctest --preset debug --output-on-failure
```

| File | Covers |
|---|---|
| `client_signer_test.cpp` | Envelope round-trip, tamper detection (payload and wrong-key), the too-short-input edge case, and the `SignatureVerifier`-shaped factory function. |
| `proof_verifier_test.cpp` | Accepting a genuine record built the same way `evidence/`'s signing gateway would build it; rejecting the wrong locally-retained input; the single-output convenience overload. |
| `alarm_test.cpp` | Not-yet-overdue, becomes-overdue-past-the-bound, cleared-by-`proofReceived`, and multiple concurrently-tracked sequence numbers. |
| `reconciler_test.cpp` | Matching bytes reconcile cleanly; mismatched bytes are reported as specification.md §7.3's fraud proof; not-yet-committed is distinguished from an actual mismatch; `checkAll`'s batch convenience. |
| `propose_client_test.cpp` | Two real transports: `ProposeClient` wired straight to a real, in-process node's `ProposeService` (the "pass-through, no gateway" shape); and wired through a real `InputGatewayImpl` configured with `client_signer.hpp`'s own `makeEnvelopeSignatureVerifier` — proving the signer and gateway/input's verification hook actually fit together, including a hand-tampered envelope being rejected before it ever reaches `Propose`. |

## Seeing it in action

There's no standalone binary here — `sdk/` is a library an application's
own client code links, not something that runs on its own. The clearest
worked examples are the tests above:
`propose_client_test.cpp`'s `SignedEnvelopeIsAcceptedAndATamperedOneIsRejectedAtTheGateway`
is the fullest one, tying `client_signer.hpp`'s signing,
`gateway/input`'s `SignatureVerifier` hook, a real node, and a real
`InputGatewayImpl` together in one real request.
