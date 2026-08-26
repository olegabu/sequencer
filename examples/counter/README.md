# examples/counter/

The one example in this repository (specification.md
[§10](../../docs/specification.md#10-the-counter-example)): a running
total. Input is an 8-byte signed delta; state is the total; the one
output — also the designated output, since the submitting client is its
only interested party — is the new total after applying the delta.

As of §15 item 6, the full §10 layout exists: the state machine,
`node_main` (proven with a **real three-node raft group**),
`replay_main` (the determinism gate, proven in CI on every push), the
input and output codecs, both gateway mains, an open-loop load
generator, and an end-to-end test that drives all three processes
together over real sockets — the whole path from a JSON `Submit` on the
input gateway through the node's raft group to a WebSocket broadcast on
the output gateway.

## The codecs

`CounterInputCodec` (`counter_input_codec.hpp/.cpp`) parses a submitted
body as `{"delta": N}` into the 8-byte signed input the state machine
expects, rejecting anything that isn't valid JSON or is missing the
field — the gateway never calls `Propose` for a request the codec
rejects. Its `toOutput` builds the synchronous response the submitting
client sees: `{"sequence_number":N,"total":M}`. Since the protocol is
stateless (a delta doesn't depend on anything about the connection that
sent it), `onDisconnect` always returns `std::nullopt`.

`CounterOutputCodec` (`counter_output_codec.hpp/.cpp`) reads a
committed record's designated output (the new total, or `0` if the
record has none) and broadcasts the same
`{"sequence_number":N,"total":M}` JSON to every subscriber of the
`"totals"` topic — the one topic this example has, since every client
cares about the same running total.

Both codecs share `json_util.hpp`'s minimal hand-rolled field
extraction and JSON building — specification.md's dependency list has
no JSON library, and this example's wire format is one integer field
each way, not worth pulling one in for.

## The output transport: WebSocket, not brpc Streaming

`gateway/output/`'s chassis is transport-agnostic
(`sequencer::OutputTransport`); this example plugs in
`sequencer::WebSocketOutputTransport` instead of the chassis's built-in
brpc-Streaming `BrpcStreamTransport`, so a browser can subscribe
directly with no brpc client of its own — specification.md §8.7 calls
out WebSocket as the other zero-additional-dependency choice, this time
for browser-facing consumers.

That transport itself — its Beast implementation, thread-safety design,
and path-based topic routing (`ws://host:port/totals` for this
example's one topic) — now lives in
[gateway/output/README.md](../../gateway/output/README.md), not here:
it turned out to have nothing counter-specific about it once its one
hardcoded assumption (a single implicit "totals" topic every client
joined, regardless of what it asked for) was generalized, so it moved
to `gateway/output/` for any application to reuse rather than
reimplement. This example now just links
`sequencer::gateway_output_websocket` and passes
`WebSocketOutputTransport` to `RunOutputGateway`'s transport-factory
overload in `websocket_output_gateway_main.cpp` — still "the one place the
example depends on something beyond brpc" (§8.7), just via a shared
dependency now instead of a private one.

## The gRPC alternative: two patterns, one for each side

`demo_grpc.sh` is the real-gRPC counterpart to `demo.sh` — same
single-node raft group, but submission and dissemination both go over
the real, standard C++ gRPC library instead of brpc/WebSocket, driven
entirely by `grpcurl`. It shows two deliberately different patterns,
one for each side of the pipeline:

- **Output** (`grpc_output_gateway_main.cpp`): wires
  `sequencer::GrpcOutputTransport` (`gateway/output/`) into the
  *existing*, unchanged `CounterOutputCodec` — the same generic,
  reusable transport `gateway/output/README.md` describes, "just like"
  `WebSocketOutputTransport`. Its wire message is a generic bytes
  envelope, so `grpcurl` prints the codec's JSON base64-encoded inside
  `OutputRecord.payload` — correct, but not directly readable without
  decoding.
- **Input** (`grpc_input_gateway_main.cpp`): a small, *counter-specific*
  gRPC service (`CounterSubmitService`, `proto/counter_input_grpc.proto`)
  with real, distinct fields (`delta` in, `sequence_number`/`total`
  out) — the fully-typed alternative, for when an application wants
  `grpcurl` to print real business fields directly instead of an
  opaque envelope. This one reuses
  `gateway::input::detail::NodeProposer` directly rather than the
  chassis's brpc-based `SubmitService`, since that chassis's
  empty-schema `SubmitRequest` has no field to carry a delta and brpc
  doesn't support gRPC reflection anyway (see
  `gateway/output/README.md`'s and `gateway/relay/README.md`'s own
  `GrpcOutputTransport`/`RelayGrpcServiceImpl` sections for why real
  gRPC is a separate library from brpc's own Streaming RPC here).

```sh
./examples/counter/demo_grpc.sh
```

submits three deltas with `grpcurl ... CounterSubmitService/SubmitDelta`
and consumes the running total with
`grpcurl ... GenericOutputService/Subscribe`, decoding and printing
each broadcast — no `.proto` file needed on either `grpcurl` command
line, since both services enable server reflection.

### A third output flavor: brpc, needing no override at all

`brpc_output_gateway_main.cpp` completes the set alongside WebSocket
(`websocket_output_gateway_main.cpp`) and gRPC (`grpc_output_gateway_main.cpp`):
the chassis's own built-in transport (`BrpcStreamTransport`), which
`RunOutputGateway`'s single-codec-argument overload already defaults to
— so unlike the other two, this main needed no transport override at
all, just the same `CounterOutputCodec` handed straight to the chassis.
Run it exactly like the WebSocket flavor ("By hand" below), on its own
port, alongside either or both of the others if wanted — nothing about
the chassis or `CounterOutputCodec` restricts an application to one
transport at a time.

All three flavors can be observed and benchmarked directly from
`counter_load_generator` (`--output_observer=grpc|brpc|websocket`,
`--output_gateway_addr`) — see
[bench/load_generator/README.md](../../bench/load_generator/README.md)'s
"four round trips" section for the full design (`SequenceCorrelator`,
shared across all three, plus what's genuinely different per
transport).

## Testing

```sh
cmake --preset debug
cmake --build --preset debug
ctest --preset debug --output-on-failure
```

| File | Covers |
|---|---|
| `counter_state_machine_test.cpp` | The state machine in isolation: single and accumulated deltas, the designated output, rejecting a wrong-sized input, and a snapshot save/load round trip. No braft, no journal, no process. |
| `three_node_smoke_test.cpp` | **Three real `counter_node` subprocesses** — the actual compiled binary, not a stand-in — forming a real raft group. Proposes several deltas, following leader redirects exactly as a gateway would (§8.1), then reads all three replicas' journal files directly and asserts they are **byte-for-byte identical** — the concrete proof behind §3's "replicas lag, never diverge." |
| `replay_test.cpp` | Records a journal with the real `CounterStateMachine`, then replays it through a completely fresh instance via `tools/replay` and asserts byte-identical output — specification.md §11's determinism gate for this example, and what `.github/workflows/ci.yml` runs on every push. Complementary to `three_node_smoke_test.cpp`: that proves cross-*replica* determinism, this proves cross-*time* (record now, replay later, possibly after a rebuild) determinism — together, §2.1's full claim: "two replicas — or one replica and a later replay — produce byte-identical journals." |
| `counter_codec_test.cpp` | Both codecs in isolation, no gateway or process involved: `CounterInputCodec` parsing valid/negative/whitespace-tolerant/missing-field/non-JSON bodies and building its response JSON; `CounterOutputCodec` broadcasting a record's total (and defaulting to `0` for a record with no outputs) to the `"totals"` topic of a recording `Fanout` test double. |
| `end_to_end_test.cpp` | The full pipeline, four real processes (`counter_node`, `counter_input_gateway`, `counter_websocket_output_gateway`, plus a test WebSocket client connecting to the `"totals"` topic) and one real submitting `brpc::Channel`: submits three deltas through the input gateway, asserts each synchronous response and each WebSocket broadcast carry the identical, correctly-accumulating `{"sequence_number":N,"total":M}` JSON — specification.md §15 item 6's deliverable, and the closest thing in this repository to a full deployment. Verified with 10+ consecutive clean runs beyond its first pass, following this session's pattern for anything involving subprocesses and networking. |
| `kill_leader_drill_test.cpp` | specification.md §14's acceptance-checklist items 2 and 5: a real 3-node cluster, a client proposing continuously exactly as an input gateway would (follow redirects, retry on failure), and an abrupt `SIGKILL` — of the leader in one test, of a follower in the other — while load is in flight. Verifies dense sequence numbers straight through the fault, byte-for-byte agreement between every surviving (and the killed node's own already-committed) journal, and continued commits after the kill. Deliberately never asserts an exact post-kill total — see the file's own header comment for why, given `CounterStateMachine` has no idempotency-key deduplication. |

## A real bug this test caught

`three_node_smoke_test.cpp` is the first test in this repository to run
more than one node process at once, and it immediately found a real bug
that no single-node test could: `CommittedEntryRing`'s default sizing
(4096 slots × 1 MiB) reserved exactly 4 GiB per node, and
`std::make_unique<std::byte[]>` **value-initializes** — an explicit,
non-lazy zero-fill of every byte, not a lazy mmap page-fault cost like
`journal/`'s sparse file reservation. Three nodes each blocking for
seconds on that (worse under this development VM's memory contention)
starved the very raft replication threads leader election depends on,
so the group never formed within any reasonable test timeout. Fixed in
`node/src/committed_entry_ring.hpp` (default-initializing allocation
instead of value-initializing) and by shrinking the default per-input
cap to 64 KiB (`node/src/node_impl.hpp`) — a more proportionate default
regardless, since realistic client-signed commands are nowhere near
1 MiB. This also incidentally cut `node/`'s own single-node integration
test from several seconds to about a tenth of a second.

Separately, running several node processes at once on one shared
development machine surfaces a scenario specification.md §5.4
anticipates directly: the pinned apply thread's mandatory, unconditional
busy-spin (one core at 100% *per node*, by design) can starve raft's own
replication threads when multiple nodes share a small machine. §5.4
explicitly permits "a park-capable mode... an explicit, clearly-labeled
concession for local development, never as a default" for exactly this
case — implemented as `ApplyLoop::run()`'s `pureSpin` parameter and the
`--apply_thread_pure_spin` flag (default `true`, i.e. spin), which this
test sets to `false` for its three subprocesses.

## A dangling-temporary bug this phase's tests caught again

`counter_codec_test.cpp` hit the same class of bug journal's own tests
caught earlier: a `Payload`/view constructed from a `std::string`
temporary (e.g. `payloadOf(R"({"delta": 5})")`) that's still read on a
*later* statement — the temporary is destroyed at the end of the
statement that constructed it, so the view dangles. Fixed by binding
the literal to a named `const std::string` local first in every
affected test case. Worth calling out again here since it's now
recurred three-plus times across this codebase; the fix is always the
same, and always easy to miss in a one-line test setup.

## Benchmarking the output gateway: four round trips, three real bugs, one reverted rewrite

`bench/load_generator/`'s harness measures four round trips end to end
against a live fleet, not just the synchronous ack path: submission to
consensus ack (always on), to the relay gateway's own gRPC stream
(`RelayObserver`), and — added this round — to each of the three
output-gateway transports (brpc, real gRPC, WebSocket), via
`SequenceCorrelator` and three concrete `OutputGatewayObserver`s. The
missing brpc-flavored counter binary (`counter_brpc_output_gateway`,
`brpc_output_gateway_main.cpp`) was added alongside it — the transport
itself already existed as `RunOutputGateway`'s chassis default
(`BrpcStreamTransport`), just never exposed as its own counter binary
before. Flag-controlled on the load generator: `--output_observer=grpc
|brpc|websocket` plus `--output_gateway_addr`.

**This benchmark was the first thing to ever load-test the output
gateway at real throughput**, and it found three real, pre-existing
bugs in production code, none specific to the new observer harness:

1. **`StreamFanout::write()` silently dropped messages on `EAGAIN`.**
   Expected backpressure under real load, treated identically to "the
   client is gone" — a benchmark subscriber at 70k records/sec received
   almost none of them. Fixed with a short (~10ms) bounded retry.
2. **`make clean-data` (raft-tests/) left output-gateway resume files
   behind.** A resume file surviving while the journal it referenced
   didn't left a gateway waiting forever for a sequence number the
   fresh journal would never reach — an entire sweep point silently
   reporting zero completions with no error anywhere. Fixed by also
   clearing `output_resume_*` there.
3. **`ResumePosition::store()` — a full open/write/close/rename
   filesystem cycle — ran after every single delivered record.** This
   was the dominant cost of the whole tailing loop: an output gateway
   couldn't sustain even 10k records/sec, falling permanently behind
   and never catching up within an entire benchmark run. Fixed by
   persisting at most every 1000 records or 200ms, with a final,
   unconditional persist on graceful `stop()` so the existing
   restart-without-redelivering guarantee stays exactly true.

**Then, with the harness actually working, both real transport
bottlenecks the relay gateway had already been through this session
showed up here too** — see [gateway/relay/README.md](../../gateway/relay/README.md)'s
"Batching the gRPC stream" section for that original story:

- **`GrpcOutputTransport`** sent one `OutputRecord` per `Write()` call
  — measured **~1.66 seconds** p50 at 70k msg/s despite 100% delivery
  (no drops, just enormous queueing delay). Fixed by batching into
  `OutputRecordBatch` — the same wire-protocol change the relay's own
  gRPC Subscribe got. Confirmed: **~4.3ms** at the same rate afterward.
- **`OutputGatewayImpl::tailLoop()`** called the transport once per
  record too, for every transport, not just gRPC — live profiling
  (brpc and gRPC showing near-identical ~4.7-5ms p50 at 100k req/s
  despite completely different wire mechanics) pointed at this shared
  loop rather than either transport specifically: `tailLoop()` itself
  was barely 1% of CPU self-time in the profile, almost all of it was
  real TCP/socket-write syscall path plus kernel spinlock contention.
  Added `Fanout::flush()` (default no-op) and had `tailLoop()` gather
  a batch before calling it once, instead of once per record.
  `StreamFanout` (brpc) implements it by accumulating into a
  length-prefixed per-stream buffer, not a protobuf envelope
  (deliberately — this transport's whole point is carrying an
  `OutputCodec`'s bytes completely unmodified) — decoded transparently
  by the shared test helper and the new observer. Caught immediately
  by the existing tests: `BrpcStreamTransport` never overrode
  `flush()`, so nothing accumulated ever actually got sent until that
  was added too. Confirmed: brpc **4731us → 4207us** at 100k req/s.

**A further experiment — an artificial bounded batching delay — did
not pay off, honestly reported rather than adopted.** At a steady,
caught-up 100k req/s (not a genuine backlog), `tailLoop()`'s "grab
whatever's available this instant" gather naturally forms small
batches — records arrive roughly evenly spaced, not bursty — so the
per-record write-syscall count barely drops versus fully unbatched.
Added `--batch_window_us` (`OutputGatewayConfig::batchWindow`,
default 0/off) to let the loop keep gathering for up to N microseconds
once at least one record is available. Measured at 200us: p50 only
4207us → 4107us (a 100us net gain against a 200us delay budget — most
of the theoretical syscall savings were consumed by the delay itself),
and **p99.9 got meaningfully worse** (9439us → 20991us) — the fixed
delay becomes a real, visible floor under every record, with more
variance than it saves. Not adopted as a nonzero default; kept as a
tunable at the time, and removed outright in the round below, where
per-subscriber reader-side draining made batching a consequence of
the design rather than something to tune.

**A follow-up round chased the relay's own `<1ms` result harder, with a
mixed outcome — one real lever paid off, one didn't, honestly reported
either way:**

- **gRPC: rewriting `GenericOutputServiceImpl::Subscribe` onto
  `grpc::ServerWriteReactor<OutputRecordBatch>` — the exact change that
  took the relay from ~4-5ms to 649us-1.3ms — made this transport
  *worse*, not better** (100k: 4731us reactor vs 4347-4951us sync
  range across runs; 70k: 4195us reactor vs 4347us sync — marginal at
  best, a regression at 100k). Root cause, found by comparing the two
  producers rather than by guessing again: the relay's own win came
  specifically from unblocking a producer thread that was *also* the
  writer thread — one thread did journal-polling **and** the blocking
  `Write()` call, so the reactor rewrite freed that thread to keep
  polling instead of blocking on the network. The output gateway's
  producer (`tailLoop()`) was already decoupled from the write call,
  via the existing queue-based `SessionRegistry::push()` (enqueue and
  return, never blocks) — so the specific problem the reactor rewrite
  fixes for a blocking-write producer never existed here. Reverted
  (`git revert` — history preserved, not squashed away) rather than
  kept as a strictly-worse alternative.
- **WebSocket: `WebSocketOutputTransport` had never been touched by any
  of this round's batching work** — `broadcast()`/`toSession()` called
  `postWrite()` (one real `net::post()` thread hand-off) per record,
  unconditionally, and `Fanout::flush()`'s no-op default meant
  `tailLoop()`'s own batching did nothing for it. Given the same
  `append()`-then-`flush()` treatment as `StreamFanout` (length-prefixed
  accumulation per session, one websocket text frame per `flush()`
  instead of one per record). Confirmed: **~5.0ms → 3879us at 70k**
  (~23%, a bigger relative win than brpc's own ~11% from the same
  pattern), and 4331us at 100k — now in line with the other two
  transports rather than trailing them.
- **brpc was re-checked at 100k (4187us, matching its earlier 4207us —
  stable, as expected since nothing in this round touched its own code
  path) rather than re-profiled with `perf` a second time.** The
  earlier live profile already pinned its cost on the real TCP/
  socket-write syscall path and kernel spinlock contention, not
  application code, and `StreamWrite()`'s own non-blocking API leaves
  no "stop blocking the producer" lever the way gRPC's sync API did —
  so a repeat profile would very likely just reconfirm the same
  finding rather than surface something new to chase.

**Where that round left the four round trips, all measured at 100k
req/s against the same live fleet** (4x c6i.2xlarge; braft's own bare
consensus latency, raft-tests/sweep/knee-sweep.csv, is the floor these
are all measured against — no sequencer/gateway layer at all):

| round trip | p50 @ 100k |
|---|---|
| braft, bare consensus (no sequencer layer) | ~749us |
| sequencer's own synchronous ack | ~1.6ms |
| relay gateway (gRPC) | ~1.3-1.4ms |
| output gateway, brpc | ~4.2ms |
| output gateway, gRPC (batched, sync API) | ~4.9ms |
| output gateway, WebSocket | ~4.3ms |

Output gateways went from broken (dropped messages, or multi-second
queueing delay) to correct and reasonably fast, and WebSocket closed
most of its gap with the other two, but all three remained well above
sub-millisecond — the `<1ms`-at-100k bar the relay gateway (mostly)
cleared after its own reactor rewrite. That specific lever doesn't
transfer here (see above), and brpc's own profiling points at
syscall/kernel-level cost rather than anything left to batch in
application code. What the evidence pointed at instead was the
output gateway's whole single-shared-tailing-thread design — which is
what the next round actually changed.

## Rebuilding the output gateways on per-subscriber ring readers

The push design had one tailing thread run the codec and then push
every record into per-session queues, where each transport's own
writer thread later picked it up — a mutex hand-off plus a
cross-thread wake before any socket write. Measured queue high-water
marks of 700-2500 records at 100k (multi-ms of pure queueing) and a
profile blaming syscalls and kernel locks both pointed there. The
relay never had this problem because it never had the hand-off: its
subscribers pull the mmap'd journal directly through private cursors.

The output gateway can't pull the journal directly — unlike the relay
it must *decode* each record to route it (`Fanout::toSession` exists
precisely so a codec can address one session, e.g. an execution
report reaching only the two sides of a trade), and §8.3 requires the
codec run exactly once per record, in order. So the codec stays on
one thread and publishes into a new **`BroadcastRing`**
(`gateway/output/include/sequencer/broadcast_ring.hpp`): a
single-writer ring where every subscriber holds its own private
cursor and filters entries by a routing tag. Deliberately not an SPMC
queue — nobody competes for entries, so no CAS anywhere; each
producer/reader pair is plain acquire/release, the same discipline
`node/src/committed_entry_ring.hpp` already uses, extended from one
consumer to N. The producer never waits on a slow reader (overwrite
semantics, seqlock-style lap detection); a lapped subscriber is
disconnected rather than silently corrupted or allowed to accumulate
unbounded backlog. Payload words are copied as relaxed atomics rather
than raw `memcpy` so the deliberate tear-then-discard is
standard-conforming — TSan flags the textbook seqlock, correctly.

Each transport then gives every subscriber its own reader thread
draining that ring straight to its own socket. For gRPC this finally
makes the *synchronous* API the right tool: its thread-per-`Subscribe`
call, the very shape the reverted reactor rewrite was trying to escape,
is exactly the per-subscriber thread this design wants. WebSocket
satisfies Beast's "shared objects: unsafe" rule by handing the stream
to its writer thread outright instead of posting onto a shared io
thread, which retires the per-connection write queue as well.
`--batch_window_us` is gone entirely — reader-side draining batches
naturally, with no artificial delay — and the tailing loop's flat 5ms
idle poll became spin-then-back-off.

One race is worth naming because it reproduced as a 1-in-4 test flake
before being pinned down: a subscriber's start cursor must be captured
on the *registering* thread, not inside the newly-spawned reader. A
late-scheduled reader that reads `head()` itself silently drops
everything published between registration and its first scheduling
slot, treating it as pre-subscription history.

**Result, on 4x c7a.2xlarge (AMD Genoa ~3.7GHz — the fleet was
upgraded for per-core speed in the same round, so these are not
directly comparable to the c6i numbers above):**

| round trip | p50 @ 100k | safe ceiling |
|---|---|---|
| sequencer's own synchronous ack | ~1.2-1.3ms | — |
| relay gateway (gRPC) | ~915us | 120k |
| output gateway, brpc | ~887us | — |
| output gateway, gRPC | ~894us | — |
| output gateway, WebSocket | ~871-892us | 120k |

All three output transports now clear the `<1ms` bar and land at or
slightly below the relay's own number, with the same 120k safe
ceiling (125k collapses). Isolating architecture from hardware: the
relay's code did not change this round and moved ~1.3ms → ~915us
(~1.4x, hardware alone), while the output gateways moved ~4.2-4.9ms →
~890us (~4.7-5.5x) — so the ring redesign accounts for roughly a
3.4-4x improvement on top of the faster cores.

The one honest asterisk is on the *ack* path, not the gateways: it
measures ~1.2-1.3ms standalone but ~910us when an observer runs in
the same load-generator process — reproducible, with the ack
histogram computed identically either way (so not a measurement
artifact), and CPU-idle effects on the node ruled out by experiment.
Unexplained as of this writing, and worth resolving before the ack
path is quoted as sub-millisecond.

## Seeing it in action

### The fastest way: `demo.sh`

```sh
./examples/counter/demo.sh
```

Starts a real single-node raft group, a real input gateway, and a real
output gateway, then drives the whole pipeline using tools *outside*
this repository entirely — `curl` to submit three deltas as plain
HTTP+JSON, [`websocat`](https://github.com/vi/websocat) to watch the
running total arrive live over plain WebSocket — and checks that what
`websocat` received matches what `curl` was told, byte for byte. It's
the external-tool counterpart to `end_to_end_test.cpp` (which drives
the identical pipeline from an in-process gtest via `brpc::Channel` and
a Beast client instead): proof that the system speaks plain HTTP and
plain WebSocket to *any* client, not just this repository's own C++
code. Needs `websocat` on `PATH` — **not `curl` itself**: WebSocket
support in curl is experimental, opt-in at compile time, and absent
from most distro-packaged builds (this repository's own dev image ships
curl 7.68, which predates it entirely), so `websocat` is curl's natural
counterpart for the receiving side, not curl with a different flag.

### The real-gRPC counterpart: `demo_grpc.sh`

```sh
./examples/counter/demo_grpc.sh
```

Same pipeline, driven by `grpcurl` instead of `curl`/`websocat` — see
"The gRPC alternative" section above for what each side demonstrates.

### By hand

Run a single node directly. `--peers` is **not** defaulted to `--peer`
— despite what an earlier draft of this README claimed, an empty
`--peers` leaves the raft group entirely unconfigured (no election
timer ever starts) rather than bootstrapping a trivial one-node group,
so pass it explicitly, equal to `--peer`, to self-elect:

```sh
./build/debug/examples/counter/counter_node \
  --peer=127.0.0.1:8100:0 --peers=127.0.0.1:8100:0 --data_dir=/tmp/counter-node-0
```

Point an input gateway at it, and an output gateway at the same data
directory (colocated — see
[gateway/output/README.md](../../gateway/output/README.md) for why):

```sh
./build/debug/examples/counter/counter_input_gateway \
  --node_peers=127.0.0.1:8100 --listen_port=8200
./build/debug/examples/counter/counter_websocket_output_gateway \
  --data_dir=/tmp/counter-node-0 --resume_file=/tmp/counter-node-0/resume \
  --listen_port=8300
```

Submit a delta and watch the total accumulate: with the load generator
— a real open/closed-loop, HDR-histogram benchmark harness now (see
[bench/load_generator/README.md](../../bench/load_generator/README.md)),
not just a rate-limited firehose; a short open-loop run at 50 req/s
reporting the submission-to-synchronous-receipt round trip
(specification.md §12's load-testing guidance):

```sh
./build/debug/examples/counter/counter_load_generator \
  --input_gateway_addr=127.0.0.1:8200 --mode=open --rate=50 --warmup=2 --measure=5
```

with `curl` and `websocat` exactly as `demo.sh` does (see its
`SubmitService/Submit` URL, its `-n`/`--no-close` flag, and its
`/totals` path on the WebSocket URL — `WebSocketOutputTransport` routes
a connecting client's topic from the URL path, so `ws://host:port/`
with no path joins nothing this codec ever broadcasts to — for the
three details easy to get wrong by hand), or with any brpc client (or
`end_to_end_test.cpp`'s `submitDelta` for the exact shape).

Once a node has proposed a few deltas, its data directory holds a real
journal — inspect it with `tools/dumper`, or certify it with this
example's own replay binary:

```sh
./build/debug/tools/dumper/dumper --data_dir=/tmp/counter-node-0
./build/debug/examples/counter/counter_replay --data_dir=/tmp/counter-node-0
```

See `tools/replay/README.md` and `tools/dumper/README.md` for what
each prints and what their flags do.

`three_node_smoke_test.cpp` is the fuller multi-node demonstration —
three real processes, a real election, real `Propose` RPCs — and is the
best place to read for the exact flags a multi-node local deployment
needs (`--group`, `--peer`, `--peers`, `--election_timeout_ms`).
`end_to_end_test.cpp` is the fuller full-pipeline demonstration — one
node plus both gateways plus a real submitter and a real WebSocket
observer.

## A note on debugging multi-process tests in this environment

Getting `three_node_smoke_test.cpp` working surfaced two environment
quirks worth knowing if you're debugging node processes by hand here,
rather than through `ctest`:

- A backgrounded (`&`) process's stdout/stderr is not visible in this
  tool's captured output, even though the process runs normally —
  only a foreground command's output is captured. This has nothing to
  do with buffering or file redirection; it's specific to how output is
  captured from background jobs here.
- `gflags`/`glog`'s own flags (`--logtostderr`, `--log_dir`, etc.) are
  real and registered (`--help` lists them), but glog defaults to a
  30-second buffer flush interval, so short-lived processes often show
  empty log files even if directly redirected — pass `--logbufsecs=0`
  for immediate flushing, or query `http://<peer-without-idx>/status`
  (brpc's built-in monitoring page) directly instead of relying on logs
  at all.
