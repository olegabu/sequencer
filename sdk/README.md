# sdk/

specification.md [§9](../docs/specification.md#9-repository-layout-and-build-tooling):
"the reference client library, per language: propose, verify proofs,
reconcile against the journal, raise the proof-timeout alarm — and,
because verification requires reading the journal, the per-language
journal reader lives here rather than in a separate bindings folder."

`cpp/` is the only implementation in this repository so far — every
other component here is C++-only, and a second language's SDK (with
its own journal-reader binding) is future work, not started in this
phase. See [cpp/README.md](cpp/README.md) for what's actually built.

## A design decision worth calling out: transport-agnostic, on purpose

specification.md §9's dependency arrows are explicit: "sdk depends on
journal and evidence's proof format" — not on `gateway/` or `node/`.
Taken literally, that rules out `sdk/` shipping a concrete RPC client
for either an input gateway (application-specific wire protocol, not
something a generic library can know) or a node's `ProposeService`
(would mean depending on `node/`'s proto, which the dependency arrows
don't grant). `cpp/`'s `ProposeClient` resolves this by taking the
actual network send as a caller-supplied callback: the library
contributes exactly the parts that are always the same regardless of
transport (signing a payload once, and specification.md §8.1's
"resubmit blindly" retry loop that resends those identical bytes), and
the application wires in how those bytes actually leave the process.
See `cpp/include/sequencer/sdk/propose_client.hpp`'s file comment, and
`cpp/tests/propose_client_test.cpp` for two concrete instances of that
wiring — one straight to a node, one through a real input gateway.
