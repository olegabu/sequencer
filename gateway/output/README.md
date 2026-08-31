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

## Delivery: one ring, one reader per subscriber

The codec never touches a transport. `OutputCodec::toOutput` publishes
into a chassis-owned **`BroadcastRing`**
(`include/sequencer/broadcast_ring.hpp`), and every connected
subscriber drains that ring through a private cursor, on a thread the
transport gives it, writing straight to its own socket:

```
journal --(tailing thread: codec, once per record, in order)--> BroadcastRing
                                                                    |
                    +-------------------------+------------------- -+
                    |                         |                     |
              brpc reader thread      gRPC reader thread    WebSocket writer thread
              (per stream)            (= Subscribe()'s own) (per connection)
                    |                         |                     |
              StreamWrite               writer->Write()        ws.write()
```

This replaced a push design — one shared tailing thread pushing every
record into per-session queues, with each transport's own writer
thread picking it up later — that cost a mutex hand-off plus a
cross-thread wake before any socket write. Measured, that was where
delivery latency lived: queue high-water marks of 700-2500 records at
100k req/s, and a `perf` profile blaming syscalls and kernel locks
rather than anything in this component's own code. Rebuilding on the
ring moved p50 at 100k from ~4.2-4.9ms to ~890us across all three
transports — see `examples/counter/README.md`'s benchmark section for
the full before/after.

**Why a ring here and not the relay's design.** `gateway/relay/` has
no ring: its subscribers pull the mmap'd journal directly through
private cursors, because a relay re-serves journal bytes unchanged.
This component can't do that — it must *decode* each record to route
it (`Fanout::toSession` exists precisely so a codec can address one
session: an execution report reaching only the two sides of a trade),
and §8.3 requires the codec run exactly once per record, in order. So
the codec stays on one thread and its *output* goes into the ring;
the ring is the journal's stand-in for post-codec bytes.

**Why not an SPMC queue.** Every subscriber must see every entry, so
nobody competes for one — which is what makes the classic
multi-consumer dequeue race (and its CAS loop) unnecessary. Each
producer/reader pair is effectively SPSC over shared storage, needing
only acquire/release, the same discipline `node/src/committed_entry_ring.hpp`
already uses, extended from one consumer to N. Entries carry a
routing tag (a session id, or an interned topic id via
`TopicRegistry`), so readers filter with an integer compare and never
hash a string on the hot path.

**Slow subscribers are disconnected, not queued.** The producer never
waits on a reader — a stalled WebSocket client must not stall
dissemination to everyone else. A reader that falls a full ring behind
gets `Overrun` on its next read and its session is closed. That
surfaces a slow consumer instead of hiding it in an unbounded queue,
which is exactly where the old design's multi-millisecond delays
accumulated. `--ring_slots` × `--ring_max_payload` sets the headroom
(default 65536 × 512 = 32MB, ~650ms at 100k records/sec).

**Torn reads.** Slots are versioned seqlock-style: odd while the
producer is mid-write, even (encoding the sequence number) once
complete. A reader copies the payload out and only *then* re-checks
the version; a change means it was lapped mid-copy and the copy is
discarded as an `Overrun`. Payload words are copied as **relaxed
atomics rather than raw `memcpy`** — a textbook seqlock's plain-memory
concurrent copy is a formal data race under the C++ memory model, and
ThreadSanitizer flags it correctly even though the torn value is
always thrown away. Relaxed atomic word copies say "tearing is
expected and handled" in a standard-conforming way and compile to
plain 8-byte moves on x86.

`OutputTransport` (`include/sequencer/output_transport.hpp`) is the
extension point: `attach(ring, topics, idleSpinIterations)`, then
`start(port)` / `stop()`. It is deliberately **not** a `Fanout` any
more — `Fanout` is the codec's interface, and a transport is now on
the reading side of the ring, not the writing side.

**One gateway serves a list of transports, not one.**
`RunOutputGateway` takes a factory returning
`OutputTransportBinding`s — a transport plus the port it listens on —
so a single process can serve brpc, real gRPC and WebSocket
subscribers at once. The ring is what makes that nearly free: the
codec runs once per record and publishes once no matter how many
transports are attached, where N single-transport processes would tail
the journal N times, run the codec N times and keep N resume
positions. `examples/counter`'s `counter_output_gateway` does exactly
this (`--brpc_port` / `--grpc_port` / `--websocket_port`, 0 disabling
each).

Note this is one process on N ports, not brpc's own trick of several
protocols on ONE port: brpc can sniff a connection's first bytes
because it implements those protocols itself, whereas these are three
independent libraries each owning its own acceptor.

**Ports come from the application, not from a chassis flag.** A port
belongs to a transport, and there is a second reason too:
`gateway/input/`'s chassis already defines a `--listen_port` gflag,
gflags flags are process-global symbols, and a binary linking both
libraries fails to link outright. Each application main defines
whatever port flags it wants and passes the values in.

`MakeBrpcOutputTransport()` exposes the chassis's own built-in
transport (`src/brpc_output_transport.hpp`) for use in such a list —
brpc's full-duplex Streaming RPC, specification.md §8.7's
zero-additional-dependency choice for brpc/gRPC-aware consumers.
Clients call `OutputSubscribeService.Subscribe` (attaching a stream
first, per brpc's usual handshake pattern) to join a topic;
`Fanout::broadcast` reaches every session on that topic,
`Fanout::toSession` one specific session by id.

**A subscriber's start cursor is captured on the registering thread**,
not inside its newly-spawned reader. This is worth naming because
getting it wrong is silent: a reader that reads `head()` itself can be
scheduled arbitrarily late, and everything published in between is
then skipped as pre-subscription history. It reproduced as a 1-in-4
test flake before being pinned down.

**Batching is a consequence, not a setting.** A reader takes whatever
accumulated since its last pass — naturally larger batches under load,
single records when idle — so the old `--batch_window_us` (an
artificial delay that bought ~100us of p50 at the cost of doubled
p99.9) is gone. Readers spin briefly then back off
(`--idle_spin_iterations`), the same idiom the relay uses, replacing
the tailing loop's old flat 5ms poll.

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

**Thread ownership, not thread hand-off.** Beast's `websocket::stream`
has one hard rule — its own docs, verbatim: "Shared objects: Unsafe."
The original design satisfied that by funneling *every* connection's
operations onto one shared `io_context` thread via `net::post`, which
meant a cross-thread hand-off per delivery. The current design
satisfies the same rule by ownership instead: the io thread only
accepts connections and runs the HTTP upgrade handshake, and the
moment a connection is established the stream is handed to a dedicated
writer thread that is from then on the only thread that touches it,
calling Beast's synchronous `write()` directly. No posts, no
per-connection write queue. Since subscribers never send anything,
there is no read loop either — a departed client surfaces as an error
on the next write. `stop()` unblocks a writer parked in `write()` by
closing the underlying socket from outside, the one deliberate
exception to single-thread stream access (a raw socket close, not a
websocket operation).

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
"Streaming RPC" (`brpc::Stream`, what `BrpcOutputTransport` above is
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

**Session design — the synchronous API turned out to be the right
tool.** gRPC's synchronous server-streaming API runs each `Subscribe()`
call on its own dedicated thread for the call's entire lifetime. Under
the old push design that thread was a liability: it spent its life
draining a mutex/condvar queue the tailing thread fed, and that
hand-off was the latency. Under the ring design *that same thread is
the subscriber's reader* — it drains the ring through its own cursor
and calls the blocking `Write()` itself, which is exactly the
thread-per-subscriber shape the architecture wants anyway.

Worth recording because it was measured the hard way: `Subscribe` was
once rewritten onto gRPC's callback API (`grpc::ServerWriteReactor`),
copying the relay's own reactor rewrite, which there had been a large
win. Here it measured *worse* and was reverted. The relay's win came
from unblocking a producer thread that was also its writer thread; this
component's producer was already decoupled from the write call, so the
problem the reactor solves never existed here. The real cost was the
queue hand-off — which is what the ring removed instead.

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
then proceeded straight to destroying the `BrpcStreamFanout` those
callbacks touch; on rare, unlucky timing, `on_closed()` landed on
already-freed memory. Fixed in `BrpcStreamFanout::closeAll()`: it now
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
shape as `BrpcStreamFanout::closeAll()` — until `on_closed()` has actually
confirmed, in its own destructor, so correctness never depends on a
test remembering to do this by hand. Verified against 40 consecutive
clean full-suite runs after the fix.

## Testing

```sh
cmake --preset debug
cmake --build --preset debug
ctest --preset debug --output-on-failure
```

`tests/broadcast_ring_test.cpp` covers the ring protocol in isolation —
no transports, no journal, no chassis:

| Case | Proves |
|---|---|
| `PreservesFifoOrderAcrossWraparound` | 100 entries through an 8-slot ring arrive in order, byte-correct. |
| `TwoReadersWithIndependentCursorsBothSeeEverything` | Broadcast, not competing-consumer: each reader sees every entry. |
| `NewReaderStartsLiveAtHeadAndMissesHistory` | The live-only subscription semantics the transports depend on. |
| `StalledReaderSeesOverrunNotBlockedOrCorrupted` | A stalled reader never blocks the producer, and is told it was lapped. |
| `ProducerAndReadersRaceFifoWhenNeverLapped` | Real threads, paced so overrun is impossible: exact FIFO under concurrency. |
| `LappedReaderSeesOverrunsButNeverTornPayloads` | Real threads, unpaced 8-slot ring: every accepted read is byte-exact for its own sequence; lapping always surfaces as `Overrun`, never as silent corruption. |

The last two are the ones to re-run under `-fsanitize=thread` when
touching the ring; the split between them exists because an unpaced
producer *will* lap a reader doing per-entry work — that is the
overwrite contract working, not a bug, and the first version of the
test learned it the hard way.

`tests/output_gateway_test.cpp` synthesizes a journal directly (no node
needed — `journal::JournalWriter`, matching
`examples/counter/tests/replay_test.cpp`'s pattern) and drives a real
`OutputGatewayImpl` with a real streaming client:

| Case | Proves |
|---|---|
| `DeliversLiveRecordsInOrderToAConnectedSubscriber` | A connected subscriber receives newly-appended records, in order, byte-correct. |
| `ResumesFromDurablePositionAfterRestartWithoutRedelivering` | Stop the gateway mid-stream, append more records, start a fresh instance pointed at the same resume file: a newly-connecting subscriber sees only the new records, never a redelivery of what was already processed before the restart. |
| `OneGatewayServesTwoTransportsFromOneJournalTail` | A single gateway bound to both a brpc and a WebSocket port: subscribers on each receive the identical record sequence from one journal tail, one codec pass and one ring. Its payloads are raw 8-byte integers on purpose — that is what caught the text-framing bug described above. |

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
| `DeliversLargeAndNonUtf8Payloads` | A 300-byte payload and one containing every byte value 0-255 both arrive intact. Regression for the text-vs-binary framing bug below; it hangs against the old behaviour rather than failing an assertion. |

**Frames are binary, and that is load-bearing.** This transport sent
*text* frames until a chassis test served one journal to a brpc and a
WebSocket subscriber at once and found the brpc one receiving records
the WebSocket one never did. RFC 6455 requires a text frame's payload
to be valid UTF-8; Beast enforces it, the write fails, and because
fanout delivery is best-effort the payload is then dropped with
nothing logged. Two things here are routinely not valid UTF-8: any
payload that simply isn't text (this transport's contract is carrying
an `OutputCodec`'s bytes unmodified, and nothing says they're text),
and the 4-byte length prefix itself, which contains a byte >= 0x80 for
any payload of 128 bytes or more. The counter example's ~40-byte JSON
happened to dodge both, which is why this survived a full benchmark
sweep undetected. Consumers now see Blob/ArrayBuffer in a browser
rather than a string — the correct shape for a length-prefixed batch
anyway.

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
`RunOutputGateway`; `--websocket_port` selects `WebSocketOutputTransport` — see
[examples/counter/README.md](../../examples/counter/README.md) for
flags and a full worked example alongside its input gateway
counterpart, or `examples/counter/demo.sh` for a live walkthrough
driven entirely by `curl` and `websocat`. The same binary with
`--grpc_port` is the same codec and same journal wired to
`GrpcOutputTransport` instead — see `examples/counter/demo_grpc.sh` for
the `grpcurl` counterpart to `demo.sh` — and `--brpc_port` selects the
built-in `BrpcOutputTransport`. Setting several serves them all at
once from one journal tail.
