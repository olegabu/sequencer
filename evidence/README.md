# evidence/

specification.md [§7](../docs/specification.md#7-evidence-blocks-roots-and-proofs):
fixed block-cutting, the Merkle builder, and the signing gateway. Operator
evidence is entirely Merkle-based, produced outside the node by an
ordinary, colocated journal reader — "no different in kind from an
output gateway." There is no per-input operator signature and no
signing thread anywhere in a node.

## block.hpp and merkle.hpp: header-only, dependent on nothing but OpenSSL

Per specification.md §9.1, both are header-only — no braft, brpc, or
protobuf — so `sdk/`'s header-only proof-verification slice can include
them directly without linking anything heavier than a cryptography
library:

- `block.hpp`: the fixed rule ("every 1024 consecutive sequence numbers
  form one block") as pure integer arithmetic. `kBlockSize` is a
  compile-time constant, not a flag — specification.md is explicit that
  changing it is "itself a sequenced administrative command," never a
  runtime setting.
- `merkle.hpp`: leaf hashing (`hash(rawRecordBytes ‖ sequenceNumber)`,
  §7.2's exact formula), tree construction, inclusion-proof generation
  and verification, and Ed25519 root signing/verification. Every block
  is always exactly `kBlockSize` records — specification.md §7.1 never
  cuts a partial block — so every tree built here has a power-of-two
  leaf count and is perfectly balanced; there is no "promote the odd
  node" logic anywhere in this file because there is never an odd node
  to promote.

`evidence::signRoot`/`verifyRootSignature` are thin wrappers around a
generic `signBytes`/`verifyBytesSignature` pair (raw Ed25519 over an
arbitrary message) — factored out specifically so `sdk/`'s client-side
signer can reuse the identical OpenSSL code path for a completely
different message shape (a client's payload, not a Merkle root) rather
than duplicating the EVP boilerplate. See
[sdk/README.md](../sdk/README.md)'s `client_signer.hpp`.

## The signing gateway: a ready-to-run stock binary, not a library

Unlike the input and output gateways, a signing gateway needs no
application knowledge — no `InputCodec`, no `OutputCodec`, nothing to
translate. Its contract (§8.4) is entirely mechanical: read the
journal, cut blocks, build trees, sign roots, serve them. So, like the
relay gateway described in specification.md §9's repository layout,
this builds a complete, ready-to-run binary — `sequencer_signing_gateway`
— directly in this repository, rather than something an application
links.

`SigningGatewayImpl` (`src/signing_gateway_impl.hpp`) tails a colocated
journal (the same "colocated consumers may instead memory-map the
journal file directly" allowance §3 grants gateway/output and, later,
gateway/relay), and once a block is complete
(`evidence::blockIsComplete`), builds its tree and signs the root.
`EvidenceServer` (`src/evidence_server.hpp`) wraps that in a real
`brpc::Server` serving `EvidenceService.GetSignedRoot` and
`GetInclusionProof` (`proto/evidence.proto`) — specification.md §7.2's
"signed roots are themselves published... an inclusion proof is
reconstructible from public data forever," served for convenience.

**Per-block memory footprint is O(1).** Only `{bounds, root, signature}`
is retained per signed block — a block's leaves are recomputed from the
journal on demand whenever a proof is actually requested, never cached.
This is specification.md §7.2's "reconstructible from public data
forever" claim, working in practice: this gateway's memory use never
grows with how many blocks it has signed, however long it has been
running.

## Redundancy is free — proven directly, not just asserted

specification.md §8.4: "run at least two instances — determinism means
every instance produces identical output, so redundancy is free."
`signing_gateway_test.cpp`'s
`TwoIndependentInstancesReadingTheSameJournalProduceIdenticalSignedRoots`
is exactly that claim, checked: two independent `SigningGatewayImpl`
instances, same journal, same key, compared root-for-root and
signature-for-signature — byte-identical, the same proof style
`three_node_smoke_test.cpp` uses for "replicas lag, never diverge."

## Testing

```sh
cmake --preset debug
cmake --build --preset debug
ctest --preset debug --output-on-failure
```

| File | Covers |
|---|---|
| `block_test.cpp` | The fixed block-cutting rule in isolation: block boundaries, one-based indexing, and `blockIsComplete`'s "never partial" guarantee. |
| `merkle_test.cpp` | The Merkle math in isolation: every leaf of a full 1024-leaf block proves against the same root; a tampered leaf fails to prove; non-power-of-two leaf counts are rejected outright; root signatures round-trip and reject tampered roots/bounds; `verifyInclusionProof` accepts a real proof and rejects a substituted record, an out-of-bounds sequence number, and a tampered signature. |
| `signing_gateway_test.cpp` | A real `SigningGatewayImpl` tailing a directly-synthesized journal (no node needed, matching `gateway/output/tests/output_gateway_test.cpp`'s pattern), signing a real block, served over a real brpc client via `EvidenceService` — plus the redundancy proof above, and confirmation that an incomplete block is never signed. |

## Seeing it in action

```sh
./build/debug/examples/counter/counter_node \
  --peer=127.0.0.1:8100:0 --data_dir=/tmp/counter-node-0
./build/debug/evidence/sequencer_signing_gateway \
  --data_dir=/tmp/counter-node-0 \
  --private_key_hex=1111111111111111111111111111111111111111111111111111111111111111 \
  --listen_port=8400
```

(64 hex characters — a raw 32-byte Ed25519 seed. `--private_key_hex`
has no default; every instance signing the same journal must use the
same key for their roots to agree byte-for-byte — see §8.4's redundancy
note above.) Once the journal has committed at least 1024 records, query
a signed root or an inclusion proof with any brpc client speaking
`EvidenceService` (`proto/evidence.proto`) — `signing_gateway_test.cpp`
shows the exact request/response shape.
