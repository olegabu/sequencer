# gateway/input/

The input side's generic chassis (specification.md
[§8.5](../../docs/specification.md#85-plug-interfaces-application-facing-symmetric-by-design),
[§8.6](../../docs/specification.md#86-why-these-interfaces-live-in-the-library-not-the-application)):
terminate a client request in whatever protocol it arrived in, translate
it via an application-supplied `InputCodec`, verify the client
signature, forward to the raft group's current leader (following
redirects), and relay the receipt back. Every line of that loop is
generic except the two calls into the codec — see
`src/submit_service_impl.hpp` for the whole thing, annotated line by
line against §8.6's pseudocode.

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
scheme is specified anywhere in the spec; choosing and shipping one is
`sdk/`'s job, a later phase. The default, `acceptAllSignatures`, is an
explicit, loudly-commented placeholder, not a real verifier — it exists
so the rest of the chassis is usable and testable before `sdk/` lands.
**Do not deploy the default anywhere a signature must actually be
checked.**

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

## Seeing it in action

There's no runnable binary here yet — `RunInputGateway` needs a codec to
link against, and none exists in this repository until
`examples/counter`'s `CounterInputCodec` lands (specification.md §15
item 6). `NodeProposer` (`src/node_proposer.hpp`) is the piece most
worth reading standalone: it's `three_node_smoke_test.cpp`'s
ad hoc leader-following retry logic, generalized into real, reusable,
leader-caching chassis code — see its header comment for why caching
the last-known leader across calls matters for a gateway handling many
requests, unlike a one-shot test helper.
