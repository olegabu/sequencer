# gateway/output/

The output side's generic chassis (specification.md
[§8.3](../../docs/specification.md#83-output-gateway-contract),
[§8.5](../../docs/specification.md#85-plug-interfaces-application-facing-symmetric-by-design)):
tail a journal from a durably-tracked resume position, call an
application-supplied `OutputCodec::toOutput(record, fanout)` once per
record, in order, and let the codec route bytes through `Fanout` to
whichever clients care.

## This component's `journal tailing`: colocated, not relay-fed

`RunOutputGateway` reads the journal directly, via a plain
`journal::JournalReader` memory-map on `--data_dir` — specification.md
§3's explicit allowance: "colocated consumers may instead memory-map the
journal file directly." An earlier draft of this component added a
`Subscribe` RPC to `node/` so the output gateway could be a remote
*Subscribe client* instead, matching §8.5's fuller description of this
component ("journal tailing, Subscribe client, transport"); that was
reverted. `gateway/relay/` exists now (specification.md §8.2 — reads a
colocated replica's journal and re-serves `Subscribe(fromSequenceNumber)`
over the network), but this component was deliberately **not** wired to
consume it: an output gateway here still tails colocated only.
Switching to a relay-fed, remote-tailing mode is real future work, not
a gap in this phase — see `gateway/relay/README.md`.

## Transport: pluggable, brpc Streaming by default

`OutputTransport` (`include/sequencer/output_transport.hpp`) is the
extension point: a `Fanout` that also knows how to `start`/`stop` a
listener. `RunOutputGateway` takes one via an optional
`transportFactory` argument; the 3-argument overload (no factory)
defaults to the chassis's own built-in `BrpcStreamTransport`
(`src/brpc_stream_transport.hpp`), which wraps `StreamFanout` — built
on brpc's own full-duplex Streaming RPC, specification.md §8.7's
zero-additional-dependency choice for brpc/gRPC-aware consumers.
Clients call `OutputSubscribeService.Subscribe` (attaching a stream
first, per brpc's usual handshake pattern) to join a topic;
`Fanout::broadcast` delivers to every session on that topic,
`Fanout::toSession` to one specific session by id.

This is what lets an application plug in a different transport entirely
without touching the tailing loop at all — `WebSocketOutputTransport`
below is exactly that, for browser-facing consumers (§8.7 calls this
out too).

## `WebSocketOutputTransport`: brpc has no WebSocket support of its own

`include/sequencer/websocket_output_transport.hpp` +
`src/websocket_output_transport.cpp`: an `OutputTransport` built on
Boost.Beast instead of brpc's Streaming RPC, for any client speaking
plain `ws://` — a browser, `websocat`, anything — with no brpc or
protobuf tooling required (specification.md §8.7: "brpc does **not**...
implement the WebSocket protocol"). A separate library target
(`sequencer::gateway_output_websocket`), not folded into
`sequencer_gateway_output` itself, so an application that doesn't want
WebSocket never pulls in Boost.Beast/Asio just by linking the chassis —
the same reason `evidence/` and `sdk/` are their own link targets
rather than living inside `journal/`.

Originally `examples/counter`'s own file (nothing else in this
repository depends on something beyond brpc — see that example's
README). Moved here once it became clear nothing about it was actually
counter-specific, except one thing that had to change first:

**Topic routing.** `Fanout::broadcast(topic, bytes)` needs to know
which of a WebSocket transport's connected clients asked for `topic` —
but WebSocket has no subscribe handshake of its own the way brpc's
`Subscribe` RPC does (§8.5's plug interfaces assume one exists;
WebSocket is just a raw bidirectional byte stream after the HTTP
upgrade). The original counter-only version dodged this by hardcoding
a single implicit topic ("totals", matching `CounterOutputCodec`'s only
topic) that every connecting client joined regardless of what they
asked for. The shared version instead takes the topic from the
WebSocket URL's own request path, leading slash stripped:
`ws://host:port/totals` joins "totals", `ws://host:port/alerts` joins
"alerts", and `ws://host:port/` (no path) joins the empty-string topic,
which simply never matches unless an application deliberately
broadcasts to `""`.

**Delivery is live only — there is no historical replay for a client
that subscribes late.** A session only receives records published
*after* it registers; anything broadcast before that is gone (fanout
delivery is deliberately best-effort, not queued). This matches how a
real deployment behaves — clients are normally already connected when
data starts flowing — but it does mean tests must subscribe *before*
appending records they expect to receive; see the test file's own
comments for exactly where this bit an earlier draft.

The **resume position** (`src/resume_position.hpp`) is a different,
gateway-*process*-level concern: it's what makes a restarted gateway
pick up tailing exactly where it left off (specification.md §8.3:
"restartable from any sequence number with identical output"), entirely
independent of which clients happen to be connected at any given moment.

## `GrpcOutputTransport`: real gRPC streaming, since brpc doesn't have it

`include/sequencer/grpc_output_transport.hpp` +
`src/grpc_output_transport.cpp` — an `OutputTransport` built on the
real, standard C++ gRPC library, added "just like"
`WebSocketOutputTransport` above: a separate library target
(`sequencer::gateway_output_grpc`) so an application that doesn't want
gRPC never pulls in the gRPC C++ library just by linking the chassis.

**Why not brpc, given brpc already speaks the gRPC wire protocol?**
Only for *unary* calls. brpc does not implement genuine gRPC-protocol
*streaming* — confirmed against brpc's own upstream repository: ["BRPC
兼容GRPC stream"](https://github.com/apache/brpc/issues/1589) is an
open, unresolved feature request, not a shipped capability. brpc's own
"Streaming RPC" (`brpc::Stream`, what `BrpcStreamTransport` above is
built on) is a `baidu_std`-protocol-specific mechanism — not gRPC-wire-
compatible, and not consumable by a real gRPC client (`grpcurl`,
`grpc-go`, `grpc-java`, ...) at all. `proto/output_grpc.proto` is
therefore a genuinely separate codegen unit, using the standard protoc
`grpc_cpp_plugin` — this repository's one deliberate exception to its
usual `cc_generic_services=true` style (see
`docs/specification.md` §8.7).

**Generic, exactly like WebSocket, for the same reason.** The chassis
never interprets an `OutputCodec`'s bytes, so `output_grpc.proto`'s
`OutputRecord` message wraps them verbatim (`bytes payload = 1`)
rather than declaring application-specific fields — grpcurl prints
`payload` base64-encoded, protobuf's own JSON mapping for a `bytes`
field, regardless of what's actually inside. An application wanting
grpcurl to print real, distinct business fields directly defines and
serves its own service against its own domain types instead, the way
`examples/counter/grpc_input_gateway_main.cpp` does for submission —
see that file and `examples/counter/README.md` for the tradeoff
between the two patterns.

**Session design.** gRPC's synchronous server-streaming API runs each
`Subscribe()` call on its own dedicated thread for the call's entire
lifetime — there's no async callback machinery to build here the way
`WebSocketOutputTransport`'s single-io-thread design needs. Each
session is just a mutex/condition-variable queue that
`broadcast()`/`toSession()` push onto (from the output gateway's own
tailing thread) and the call's own thread drains, writing to its
`grpc::ServerWriter` — the one thing that *does* carry over from
`WebSocketOutputTransport` is that a `grpc::ServerWriter` (like a
`websocket::stream`) is only ever touched from its own call's thread.

**Server reflection is enabled** (`grpc::reflection::InitProtoReflectionServerBuilderPlugin()`),
so `grpcurl` needs no `.proto` file of its own to call this — unlike
brpc, which does not implement gRPC's reflection service either.

## A real, rare crash this component's tests caught

`gateway/output/tests/output_gateway_test.cpp` intermittently segfaulted
in `OutputGateway.ResumesFromDurablePositionAfterRestartWithoutRedelivering`
— reproducible only when run after many other tests in the same `ctest`
invocation, never in isolation. Root cause, found via `gdb`'s live
thread backtraces: `brpc::StreamClose()` does not guarantee its
stream's `on_closed()` callback has already run by the time it returns
— that callback can fire asynchronously, shortly afterward, from a
different thread. `OutputGatewayImpl::stop()` closed every stream and
then proceeded straight to destroying the `StreamFanout` those
callbacks touch; on rare, unlucky timing, `on_closed()` landed on
already-freed memory. Fixed in `StreamFanout::closeAll()`: it now
tracks every stream it asked to close and blocks (with a bounded
timeout, so a pathological client can never hang shutdown forever)
until each one's `on_closed()` has actually confirmed. Verified against
25 consecutive clean full-suite runs after the fix, versus a
reproducible failure within the first couple of runs before it.

## A second, symmetric crash — this time on the client side of the tests themselves

The same `brpc::StreamClose()` gap has a mirror image: a *test's own*
client-side stream handler can just as easily be freed out from under
an in-flight asynchronous `on_closed()`/`on_received_messages()`
callback if nothing waits for confirmed closure before destroying it.
This surfaced as a one-off segfault in
`tests/restart_drill_test.cpp` (specification.md §14 item 4's drill,
added later — see its own header comment) while stress-testing it: that
test tears down three `OutputGatewayImpl` instances in one process
(more than any other test here), which made the same narrow timing
window this component had already fixed once — but on the *client*
side this time, in test-only code — enough more likely to hit that it
finally showed up, after 50+ prior isolated/targeted attempts to
reproduce it found nothing. `tests/collecting_stream_client.hpp` is the
fix: a shared `Subscription`/`CollectingStreamHandler` (previously
duplicated, unfixed, in both `output_gateway_test.cpp` and this file)
whose `Subscription` now closes its stream and blocks — bounded, same
shape as `StreamFanout::closeAll()` — until `on_closed()` has actually
confirmed, in its own destructor, so correctness never depends on a
test remembering to do this by hand. Verified against 40 consecutive
clean full-suite runs after the fix.

## Testing

```sh
cmake --preset debug
cmake --build --preset debug
ctest --preset debug --output-on-failure
```

`tests/output_gateway_test.cpp` synthesizes a journal directly (no node
needed — `journal::JournalWriter`, matching
`examples/counter/tests/replay_test.cpp`'s pattern) and drives a real
`OutputGatewayImpl` with a real streaming client:

| Case | Proves |
|---|---|
| `DeliversLiveRecordsInOrderToAConnectedSubscriber` | A connected subscriber receives newly-appended records, in order, byte-correct. |
| `ResumesFromDurablePositionAfterRestartWithoutRedelivering` | Stop the gateway mid-stream, append more records, start a fresh instance pointed at the same resume file: a newly-connecting subscriber sees only the new records, never a redelivery of what was already processed before the restart. |

`tests/restart_drill_test.cpp` covers specification.md §14 item 4's
literal claim — not just "no redelivery" but "identical to an
uninterrupted run" — by running two gateways side by side: one
continuous ("gateway A"), one stopped and restarted at an arbitrary
sequence number partway through ("gateway B"), then asserting gateway
B's total delivered sequence (its pre-restart segment plus its
post-restart segment, concatenated) is byte-for-byte equal to gateway
A's.

`tests/websocket_output_transport_test.cpp` drives
`WebSocketOutputTransport` with a real synchronous Beast client:

| Case | Proves |
|---|---|
| `BroadcastDeliversToConnectedClient` | A connected client receives a broadcast to the topic it connected with. |
| `MultipleMessagesArriveInOrder` | Several broadcasts arrive in order. |
| `BroadcastToUnknownTopicIsANoOp` | Broadcasting to a topic with no subscribers is silent — no crash, no hang. |
| `TwoClientsOnDifferentTopicsOnlyReceiveTheirOwn` | Two clients connected to different URL paths each receive only their own topic's broadcasts — the path-based topic routing actually works, not just compiles. |

`tests/grpc_output_transport_test.cpp` drives `GrpcOutputTransport`
with a real synchronous gRPC C++ client (`grpc::CreateChannel` +
`GenericOutputService::Stub` — the same generated stub any real gRPC
client, including `grpcurl`, would use), covering the identical four
cases as the WebSocket table above (`BroadcastDeliversToConnectedClient`,
`MultipleMessagesArriveInOrder`, `BroadcastToUnknownTopicIsANoOp`,
`TwoClientsOnDifferentTopicsOnlyReceiveTheirOwn`) — the two transports
are held to the same bar since they implement the same
`OutputTransport` contract.

## Seeing it in action

`examples/counter`'s `counter_output_gateway` is a real, runnable
`RunOutputGateway` using `WebSocketOutputTransport` — see
[examples/counter/README.md](../../examples/counter/README.md) for
flags and a full worked example alongside its input gateway
counterpart, or `examples/counter/demo.sh` for a live walkthrough
driven entirely by `curl` and `websocat`. `counter_grpc_output_gateway`
is the same codec, same journal, wired to `GrpcOutputTransport`
instead — see `examples/counter/demo_grpc.sh` for the `grpcurl`
counterpart to `demo.sh`.
