# gateway/input/

The input side's generic chassis (specification.md
[§8.5](../../docs/specification.md#85-plug-interfaces-application-facing-symmetric-by-design),
[§8.6](../../docs/specification.md#86-why-these-interfaces-live-in-the-library-not-the-application)):
terminate a client request in whatever protocol it arrived in, translate
it via an application-supplied `InputCodec`, verify the client
signature, forward to the raft group's current leader (following
redirects), and relay the receipt back. Every line of that loop is
generic except the two calls into the codec — see
`src/request_pipeline.hpp` for the whole thing, annotated line by line
against §8.6's pseudocode.

That loop lives apart from any transport on purpose. A client-facing
protocol is an `InputTransport` (`include/sequencer/input_transport.hpp`),
mirroring the output side's `OutputTransport`, and the built-in brpc
path (`src/brpc_input_transport.hpp`) is an ordinary implementation of
it rather than a privileged one — an interface whose only implementation
is the new thing gets shaped around the new thing, and the old path then
quietly diverges. A FIX session gateway (`gateway/fix/`) plugs in the
same way.

## Why the wire format is "whatever the codec wants," concretely

`input_gateway.proto`'s request and response messages are deliberately
**empty**. brpc treats a service with an empty schema as "pure HTTP" —
the entire body lands in `Controller::request_attachment()` untouched,
regardless of whether the client spoke HTTP+JSON, gRPC, or baidu_std
(native clients set the attachment explicitly). That's what lets a
future `CounterInputCodec` accept `{"delta": 5}` directly rather than
some `{"body": "<base64>"}` envelope — the gateway never tries to parse
the body as protobuf-JSON in the first place.

## The client-signature verification hook

`signature_verifier.hpp`'s `SignatureVerifier` is a callable the
chassis invokes on every input before proposing it — specification.md
§7's "verified by default at the input gateway." No concrete signature
scheme is specified anywhere in the spec; choosing and shipping one was
`sdk/`'s job, and `sdk/`'s `client_signer.hpp` now has one:
`sdk::makeEnvelopeSignatureVerifier(publicKey)` returns a callable of
exactly this type (same underlying `std::function<bool(Payload)>`
alias, no extra dependency needed to plug it in — see
[sdk/README.md](../../sdk/README.md)), ready to pass wherever an
application constructs its `InputGatewayImpl`. The chassis's own
default, `acceptAllSignatures`, remains an explicit, loudly-commented
placeholder, not a real verifier — it exists so the rest of the chassis
is usable and testable without requiring every test to wire up real
keys. **Do not deploy the default anywhere a signature must actually be
checked** — pass a real verifier, such as `sdk/`'s, instead. See
`sdk/cpp/tests/propose_client_test.cpp` for the two plugged together
against a real gateway, including a tampered envelope being rejected
before it ever reaches `Propose`.

## Proposals are batched, and asynchronous

`SubmitServiceImpl::Submit` does not wait for the node. It runs the
codec, verifies, hands the input to `NodeProposer::proposeAsync` and
returns; the client's response is completed from the proposer's
callback. `NodeProposer` then batches proposals onto the wire —
several client requests per `ProposeBatch` RPC
(`node/proto/node.proto`) — under the rule every gateway here follows:
gather whatever is queued right now, send once, never delay to wait
for more (see [../README.md](../README.md)).

At low rates that means batches of one and unchanged behaviour. Under
load the in-flight window fills batches for free: at 100k req/s with a
~600 µs round trip there are tens of requests outstanding at any
instant, so each batch picks up whatever accumulated during the last
one. Measured at 100k: **~1.34 ms → ~1.03 ms p50**, throughput
unchanged.

**Both bounds are load-bearing**, and the first version had neither:

- *One batch in flight* measured **133 ms p50 and throughput down to
  75k**. A proposal has a round trip, so a single outstanding
  request-response pair replaced the pipelining that had previously
  spanned every in-flight client request. This is exactly where the
  analogy to `gateway/output/` stops — its batched writes are
  fire-and-forget, so one in flight costs it nothing.
- *Unbounded batch size* then let each batch swell to ~10k inputs. A
  batch's latency is its slowest member, so everyone in it waited for
  all of it. At 256 the effect is still visible (1714 µs against 1009
  at 64).

`--max_batch_size` (64) and `--max_inflight_batches` (8) expose both;
a short sweep at 100k put 32/16 and 64/8 within noise of each other
and 256/8 clearly worse.

**Why the node needed a new RPC.** `ProposeBatch` batches the *RPC*,
not consensus — braft already batches internally. Each input is still
applied as its own raft task and gets its own sequence number, so the
semantics are identical to calling `Propose` once per input. The
handler applies every task first and only then waits for all of them;
applying and waiting one at a time would serialize the batch into N
sequential commits and be strictly worse than N separate RPCs.

## Testing

```sh
cmake --preset debug
cmake --build --preset debug
ctest --preset debug --output-on-failure
```

`tests/input_gateway_test.cpp` is a real end-to-end test: a real
single-node `NodeImpl` (in-process, self-electing), a real
`InputGatewayImpl` in front of it, driven over a real brpc channel with
a raw attachment body — exactly how an external client would. Covers a
successful submit (checking both the sequence number and the designated
output round-trip correctly), a codec-rejected malformed request (never
reaches `Propose` — checked by reading the node's journal directly), and
a signature-verifier rejection (same check).

`ConcurrentSubmitsEachGetTheirOwnSequenceNumber` covers the batching
above: 24 concurrent submitters, which share `ProposeBatch` RPCs. Each
must get a distinct sequence number, and together they must cover
1..24 with no gaps or repeats. Batched results are positional, so this
is where a mis-ordered or mis-sized batch would hand a client someone
else's receipt — the journal's own committed count is checked too.

## Seeing it in action

`examples/counter`'s `counter_input_gateway` is a real, runnable
`RunInputGateway` linked against `CounterInputCodec` — see
[examples/counter/README.md](../../examples/counter/README.md) for
flags and a full worked example alongside its output gateway
counterpart and load generator. `NodeProposer` (`src/node_proposer.hpp`)
is the piece most worth reading standalone: it's
`three_node_smoke_test.cpp`'s ad hoc leader-following retry logic,
generalized into real, reusable, leader-caching chassis code — see its
header comment for why caching the last-known leader across calls
matters for a gateway handling many requests, unlike a one-shot test
helper.
