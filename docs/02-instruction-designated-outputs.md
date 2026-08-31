# Implementation instruction — designated outputs (plural) and the delivery-path rule

*For Claude Code, working in the sequencer repository. The authoritative
reference is `specification.md` as merged on 2026-08-31; section
numbers below refer to that file. Two changes landed in it that are not
yet reflected in code. Implement them in the order given; each step
names the spec section that defines it.*

## What changed, and why

1. **Designated outputs are now plural.** A state machine may mark any
   number of its emitted outputs as belonging to the submitting client's
   synchronous reply; the reply preserves emission order. The previous
   at-most-one rule cannot express an order-matching state machine's
   reply (acknowledgement, then each partial fill, then a final state —
   all the submitter's own). Counter still designates exactly one, so
   the example's behavior is unchanged; only its types are.
   Reference: **§4** (the `OutputCollector` declaration and the
   "`designateOutput`, explained" paragraph), **§5.2** (the receipt),
   **§3** (the `Propose` surface line).

2. **Which path delivers an output to a client is fixed by the
   transport's shape.** Request/response transports return designated
   outputs synchronously; session/stream transports deliver every output
   from the journal in sequence-number order and use the receipt for
   bookkeeping only. Each output reaches a given client by exactly one
   path, never both. Reference: **§8.11** (new section), with the
   contract line it adds to **§8.1** and **§8.3**.

Nothing in the journal format, the harness threading model, or the
evidence layer changes.

## Steps

### 1. `OutputCollector` — plural designation (§4)

- Change `designateOutput(size_t index)` from set-one to
  append-to-ordered-set semantics: callable any number of times, any
  order; designating the same index twice is idempotent; the resulting
  designated set is exposed in **emission order** (the order the outputs
  were `emit`ted), not the order `designateOutput` was called.
- Storage: a fixed-capacity structure sized to the arena's output
  capacity — no allocation on the apply thread, per §5.4's rule. A
  bitset over output indices is sufficient and gives emission order for
  free.
- Unit tests: none/one/many designated; out-of-order designation
  yields emission order; duplicate designation is idempotent;
  designating an index not yet emitted is a hard error.

### 2. The acknowledgement and `Receipt` (§5.1, §5.2, §3)

- `Receipt` carries `designatedOutputs` as an ordered sequence of byte
  spans (empty allowed), replacing the optional single payload.
- The harness's acknowledgement path (§5.1's `acknowledge{…}` step)
  copies the designated outputs out of the arena into the reply before
  the arena is reused for the next apply — this copy already existed
  for the single case; it becomes a loop. Verify no allocation regression
  on the apply thread: the copy destination is the brpc reply buffer,
  owned by the brpc worker, not the apply thread.
- `Propose`'s reply message: the single optional `designated_output`
  field becomes `repeated bytes designated_outputs`. This is a wire
  change for every existing client of `Propose` — update the counter's
  load generator and any gRPC/REST clients in `examples/counter`
  accordingly (§10). Keep the proto field number of the removed field
  reserved.

### 3. `InputCodec::toOutput` (§8.5, §8.6)

- Signature becomes
  `Bytes toOutput(const Receipt&, std::span<const Payload> designatedOutputs)`.
- `RunInputGateway`'s chassis loop (§8.6 pseudocode) passes
  `receipt.designatedOutputs` through unchanged.
- `CounterInputCodec::toOutput` (§10): takes `designatedOutputs[0]` when
  present; must handle an empty span cleanly (a state machine that
  designates nothing). Encode as before — one JSON response — and add a
  test for the empty case.
- If `examples/counter/grpc_input_gateway_main.cpp` (§8.10) surfaces the
  designated output as a typed protobuf field, update it to the plural
  form as well; the `NodeProposer` component it reuses is unaffected.

### 4. Session-gateway delivery rule (§8.11)

This step is a **constraint on future transports** plus one guard on
existing code; it does not require building a FIX transport now.

- Add a constant, per-transport declaration of shape:
  `TransportShape { RequestResponse, SessionStream }` on the input-side
  chassis, set by each transport (brpc/REST/gRPC-unary →
  `RequestResponse`; any future FIX or streaming input →
  `SessionStream`).
- For `RequestResponse` transports, behavior is unchanged: designated
  outputs are returned in the reply.
- For `SessionStream` transports, `RunInputGateway` MUST NOT hand
  designated outputs to the codec for client delivery: it consumes the
  receipt for admission confirmation and sequence-number bookkeeping and
  discards `designatedOutputs`. Enforce this in the chassis, not by
  convention, so that a future FIX input transport cannot accidentally
  double-deliver. A unit test with a stub `SessionStream` transport
  asserts the codec's `toOutput` is never invoked with a non-empty
  designated set.
- Document, in the output-gateway chassis (`gateway/output`), the
  corresponding half: for a session transport, **all** outputs addressed
  to a session — designated or not — are delivered from the journal in
  sequence-number order. `OutputCodec` and `OutputTransport` need no
  code change for this; add the invariant to their interface comments
  and the `Fanout::toSession` contract.
- Add the contract line from §8.11 verbatim to the input- and
  output-gateway header comments: *"a gateway delivers each output to a
  given client exactly once, by the path its transport shape dictates,
  and never by both."*

### 5. Acceptance

- All existing tests pass with the plural types; counter's end-to-end
  test (§10) still observes one designated output per delta via the
  REST and gRPC paths.
- Determinism replay (§11) is unaffected and still byte-identical —
  designation is not journaled, so the journal cannot change; assert
  this explicitly rather than assume it.
- The `SessionStream` guard test from step 4 passes.

## Out of scope for this instruction

- Building a FIX input or output transport — now specified in §8.12
  and covered by its own instruction (`03-instruction-fix-gateway.md`);
  do it there, after this one, since §8.11's `TransportShape` guard
  (step 4) is a prerequisite it relies on.
- Any change to the journal format, including the storage-hardening
  checksum/chain fields specified in `01-storage-hardening.md`.
