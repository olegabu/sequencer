# gateway/relay/

specification.md [§8.2](../../docs/specification.md#82-relay-gateway-contract),
[§9](../../docs/specification.md#9-repository-layout-and-build-tooling):
reads a colocated replica's journal via local memory-map only, and
re-serves `Subscribe(fromSequenceNumber)` over the network with
byte-identical records — the piece that lets output gateways and the
signing gateway run on a different machine than the node they consume,
without ever touching that node directly (§3.3's fan-out asymmetry
mitigation). Before this existed, every consumer in this repository
(`gateway/output`, `evidence`'s signing gateway) had to be colocated
with a node itself — the "colocated consumers may instead memory-map
the journal file directly" allowance §3 grants as a stopgap, not a
long-term substitute for a relay.

**This phase does not wire `gateway/output` or `evidence` to consume a
relay instead of tailing colocated** — both remain colocated-only, by
explicit choice, so this is additive: a new, independently-tested
component, not a change to anything else's behavior.

## Why per-subscriber cursors, not a shared broadcast

`gateway/output`'s `Fanout` delivers *live only* — a client that
subscribes late misses everything broadcast before it connected (see
`gateway/output/README.md`). A relay cannot work that way:
`Subscribe(fromSequenceNumber)` must serve *any* already-committed
sequence number, not just "from now on," so historical replay and live
delivery are the same mechanism, not two different code paths.

Concretely, `RelaySession` (`src/relay_session.hpp`) is one per active
`Subscribe` call: it owns a dedicated tailing thread reading from a
shared, colocated `journal::JournalReader`, starting at whatever
`fromSequenceNumber` that particular client asked for. Many sessions
read the same `JournalReader` concurrently — safe and cheap, since
that's exactly the "any number of independent, concurrent readers"
type `journal/` was built for (§6.4). This is what "support any number
of concurrent remote subscribers, since fan-out is precisely the load
this gateway exists to absorb off the node" (§8.2) means in practice:
each subscriber's pace and starting point are entirely its own.

## A third instance of the StreamClose()/on_closed() race — caught before shipping this time

`gateway/output/README.md` documents two real, previously-shipped
segfaults from the same root cause: `brpc::StreamClose()` does not
guarantee its stream's `on_closed()` callback has already run by the
time it returns. `RelaySession`'s destructor and
`RelaySubscribeClient`'s destructor (`include/sequencer/relay/subscribe_client.hpp`,
`src/subscribe_client.cpp`) both follow the same fix applied there:
close the stream, then block — bounded, via a condition variable —
until `on_closed()` has actually confirmed, before anything it touches
can be freed. Given this pattern has now caused real crashes twice
already in this repository (once server-side in `gateway/output`, once
client-side in that component's own tests), it was applied here
proactively, on both the server session and the reference client, from
the start — verified against 25 isolated and 15 full-suite consecutive
clean runs before this component was considered done.

## A second RelayService, in real gRPC — served on a second port

`proto/relay_grpc.proto` + `src/relay_grpc_service_impl.hpp`: the same
`Subscribe(fromSequenceNumber)` contract, served over the real,
standard C++ gRPC library instead of `brpc::Stream`, alongside (not
instead of) the brpc-based `RelayService` above. Gated by
`--grpc_listen_port` (0, the default, disables it — `sequencer_relay`
serves only the original brpc-based service unless this is set).

**Why a second server on a second port, not the same one:** brpc and a
real `grpc::Server` are two entirely separate server stacks — brpc's
own gRPC compatibility (serving `baidu_std` + REST + gRPC "on one
port," per `docs/specification.md` §8.6) is *unary-call-only*. brpc
does not implement genuine gRPC-protocol *streaming* — confirmed
against brpc's own upstream repository: ["BRPC兼容GRPC
stream"](https://github.com/apache/brpc/issues/1589) is an open,
unresolved feature request, not a shipped capability. So relay's
existing `RelayService` (built on `brpc::Stream`, a `baidu_std`-
protocol-specific mechanism) was never actually consumable by a real
gRPC client — a genuine gap for exactly the use case relay itself
exists for (§3.3: gateways, potentially written in other languages,
consuming a relay instead of touching a node directly). This closes
that gap with the real gRPC C++ library.

**Simpler than `RelaySession`, structurally.** gRPC's synchronous
server-streaming API runs each `Subscribe()` call on its own dedicated
thread for the call's entire lifetime, so `RelayGrpcServiceImpl::Subscribe`
just writes the tailing loop directly — no separate session object, no
async callback machinery, no thread to spawn (gRPC already provides
one per call). `RelayGatewayImpl::waitForReader()` is the one small
addition this needed: direct colocated read access for a caller that
isn't going through `startSession`'s brpc-specific session bookkeeping.

**Server reflection is enabled**, so `grpcurl` needs no `.proto` file
of its own to call this — unlike brpc, which does not implement gRPC's
reflection service either.

## Batching the gRPC stream — two real throughput bugs, found via live-fleet benchmarking

`RelayGrpcServiceImpl::Subscribe` originally sent exactly one journal
record per `grpc::ServerWriter<Record>::Write()` call. A live sweep
against a 3-node fleet (`raft-tests/sequencer/`, phase 3 —
`bench/load_generator/`'s `RelayObserver`) turned up a severe cliff: p50
flat at 3-4ms through 25k req/s, then 7.3 **seconds** at 40k. Two
independent bugs, both fixed:

**1. No batching, at a record size where per-call overhead dominates.**
A counter-benchmark record is ~40-50 bytes on the wire — small enough
that a blocking synchronous `Write()` call's own fixed overhead (framing,
flow-control bookkeeping, completion-queue synchronization), not payload
transmission, was the real cost per record. Fixed by gathering every
record already available (up to `--relay_max_batch_records`, default
1024 — ~50KB, comfortably under gRPC's 4MB message cap) into one
`RecordBatch` before writing, same "batch what's ready, never delay a
send to wait for more" discipline this repo already uses for braft's own
`LEADER_BATCH`/`APPLY_BATCH` tuning. Caught up, this degrades to exactly
the old one-record-per-`Write()` behavior; behind, it collapses however
large the backlog is into far fewer `Write()` round trips. `RecordBatch`
(`proto/relay_grpc.proto`) replaces the old single-`Record` message —
a wire-protocol change, confirmed safe since the only real gRPC
consumers in the tree are this repo's own test client and
`RelayObserver`, both updated alongside it.

**2. The fix's own gather loop introduced a second, much worse bug.**
The per-record gather loop re-checked `context->IsCancelled()` on every
iteration, not just once per batch (the outer loop already did that).
`IsCancelled()` isn't a cheap flag read — it plucks gRPC's own
completion queue under the hood (confirmed via `gdb` thread backtraces
on a hung local reproduction: the gather loop was spending nearly all
its time inside `grpc::CompletionQueue::TryPluck`, not in the actual
record read). Once per batch this is negligible; once per record it
dominated everything, measured at roughly **3000x** slower backlog
catch-up (a 500k-record local backlog test: ~170 records/sec with the
bug, 3-4 million/sec after removing the redundant per-record check —
see `gateway/relay/tests/relay_grpc_test.cpp`'s
`BacklogCatchUpThroughputStaysOffTheGatherLoopFloor`, added as a
permanent regression guard against a repeat of this exact class of bug).

**Confirmed on the live fleet, both fixes together** (an earlier
`seq-relay.csv`, since superseded by the reactor rewrite below): p50
flat at 3.1-5.1ms from 10k through 115k req/s, then a real cliff at
130k — landing at essentially the *same* rate as phase 1's own knee
(submission to synchronous consensus ack, no relay involved, flat to
~115k, plateau ~123-126k; see `raft-tests/sweep/mkcharts.py`'s
`"sequencer"` CFG entry). The relay no longer bottlenecked ahead of
sequencer's own consensus/journal path — but 3-5ms of *relay-added*
latency on top of a ~1-1.5ms ack path was still a lot, prompting the
next round.

## Rewriting Subscribe onto gRPC's callback/reactor API

Batching fixed throughput; it didn't fix latency the way a blocking
synchronous `Write()` call still could. Every `Write()` ties up a
dedicated OS thread for the full completion-queue round trip of that
one call — the same non-blocking-queue-and-return shape
`brpc::StreamWrite` already has on the `RelaySession` side, but the
*sync* gRPC streaming API doesn't. Rewrote `Subscribe` onto
`grpc::ServerWriteReactor<RecordBatch>` (`RelaySubscribeReactor`,
`relay_grpc_service_impl.hpp`): a dedicated "pump" thread still waits
for the journal to become readable and spin-then-backs-off when idle
(the same idiom `bench/load_generator`'s own
`RelayObserver::waitForTag` already uses — sub-millisecond common
case, not a flat 5ms poll interval), but writes now go through
`StartWrite()`/`OnWriteDone()` instead of a blocking call, and
`OnCancel()` replaces polling `context->IsCancelled()` entirely (no
equivalent of bug #2 above is even possible here — there's nothing
left to poll).

**Result, live fleet, both fixes plus the reactor rewrite**
(`raft-tests/sequencer/seq-relay.csv`): p50 649us-1.3ms from 10k
through 100k req/s — comfortably under phase 1's own synchronous-ack
p50 at the same rates (the relay tails the journal directly; the ack
path pays an extra hop back through the input gateway the relay
doesn't). This did *not* reach the sub-1ms-at-100k target exactly
(measured 1.27-1.4ms at 100k across repeated runs), and it came with a
real trade-off: the safe throughput ceiling moved from 130k **down**
to 115k — worse, not better, than the simpler batched-but-synchronous
version above. Two follow-up experiments, both against that open
question:

- **Chaining writes directly from `OnWriteDone`, skipping the pump
  thread's wake-up entirely**, on the theory that the cross-thread
  condvar wake/schedule round trip between a gRPC callback thread and
  a parked pump thread was the ceiling's cause. Measured worse, not
  better: p50 at 100k went from 1272-1383us to 1754us, with no change
  to the 115k ceiling. Reverted (see git history) — kept the simpler,
  better-measured pump-thread-driven version instead.
- **Sweeping `--relay_max_batch_records`** (128 / 1024 / 8192) at
  100k req/s: p50 barely moved (1438 / 1424 / 1264us) — a mild edge
  for larger batches, not the multi-hundred-microsecond lever the
  ceiling would need to explain itself. 128 showed a much worse tail
  (p99 24.7ms vs ~3-4ms at 1024/8192) and a hugely inflated
  `RelayObserver` queue depth (253k vs ~1.5k), consistent with smaller
  batches also costing the *client* more `Read()` calls for the same
  data — a real reason to avoid going too small, not a reason to
  believe batch size explains the 115k ceiling.

**A third follow-up used actual profiling instead of guessing.**
`perf record -F 999 -g` against the relay process (a RelWithDebInfo
build — the deployed Release binary is stripped, `-s`, useless for
symbol resolution) while driving load in the transition zone between
the relay's healthy 100k and its broken 115k found something real:
`RelaySubscribeReactor::pumpLoop()` alone accounted for **15.45% of
all CPU self-time** in the process — nearly 4x every individual gRPC-
internal function (each under 1.5%), and second only to kernel
scheduling overhead (`finish_task_switch`/futex wait+wake, ~27%
combined). The obvious suspect inside `pumpLoop()` is its own idle-wait
loop: a busy-spin of `kSpinIterations = 20000` iterations before
falling back to a 200us sleep — a real cost stolen from whatever else
runs on the same box, which matters specifically here because the
relay runs colocated with the raft leader (`raft-tests/sequencer/`'s
own placement choice, for lowest latency) — CPU the spin loop burns is
CPU the leader's own consensus work doesn't get.

Reduced the default via a new `--relay_idle_spin_iterations` flag
(20000 → 2000) and re-tested at the exact rate that broke down
(115000). The result was **not a clean fix**, and not even monotonic:

| `--relay_idle_spin_iterations` | relay p50 @ 115k req/s |
|---|---|
| 0 (no spin at all) | 1.27s |
| 2000 (new default) | 660ms |
| 20000 (old default) | 769ms |

2000 measured best of the three, a modest real improvement over the
old default and no regression at the already-healthy 100k (1358us,
in line with the 1272-1424us range measured before this change) — kept
as the new default. But every value tested still leaves 115k
thoroughly broken, and *removing* the spin entirely made things worse,
not better — consistent with the spin loop cheaply catching brief
gaps that a sleep/wake cycle's real (and likely much-greater-than-
200us, given EC2 virtualization and scheduler tick granularity) wake
latency would otherwise miss, even though it wastes CPU on longer
gaps. So the profiling finding is real and the fix is a genuine, if
modest, improvement — but it is not the explanation for the 115k
ceiling. That still stands open.

**The 115k-vs-130k ceiling regression is still not fully explained**,
now across three independent investigations (thread hand-off,
batch size, CPU-theft-via-spin-loop) that each found something true
without resolving it. Further progress would need more rigorous
methodology than live-fleet guess-and-check supports well — statistical
repetition per configuration (single runs on a shared, noisy EC2 box
are not precise enough to separate a real effect from variance, as the
non-monotonic spin-iteration result above illustrates), isolating the
relay onto its own instance rather than colocating it with the raft
leader (to separate "the relay is slow" from "the relay is stealing
cycles from something else that's slow"), and likely gRPC's own
internal tracing (`GRPC_TRACE`) rather than black-box `perf` alone. The
current defaults (`--relay_max_batch_records=1024`,
`--relay_idle_spin_iterations=2000`, the reactor rewrite as a whole)
are the best-measured trade of everything actually tried, not a
resolved story.

## Testing

```sh
cmake --preset debug
cmake --build --preset debug
ctest --preset debug --output-on-failure
```

`tests/relay_gateway_test.cpp` synthesizes a journal directly (no node
needed, matching `gateway/output/tests/output_gateway_test.cpp`'s
pattern) and drives a real `RelayGatewayImpl` with this repository's
own reference `RelaySubscribeClient`:

| Case | Proves |
|---|---|
| `SubscribingFromTheBeginningReplaysAlreadyCommittedHistory` | A client subscribing after records already exist receives all of them — no "subscribe before appending" dance required, unlike `gateway/output`'s Fanout. |
| `DeliversLiveRecordsAppendedAfterSubscribing` | The same session keeps receiving records appended after it connects — historical and live delivery are one mechanism. |
| `EachSubscriberHasAnIndependentCursorFromItsOwnRequestedSequenceNumber` | Two concurrent subscribers, one from the beginning and one from the middle, each receive exactly what they asked for, independently. |
| `DeliveredRecordsAreByteIdenticalToTheColocatedJournal` | What a subscriber receives is compared byte-for-byte against `journal::JournalReader::record(seq).rawBytes()` read directly — specification.md §8.2's literal "byte-identical records." |

`tests/relay_grpc_test.cpp` covers the real-gRPC `RelayService` above
with a real synchronous gRPC C++ client, against a directly-synthesized
journal (no node needed, matching `relay_gateway_test.cpp`'s own
pattern):

| Case | Proves |
|---|---|
| `SubscribingFromTheBeginningReplaysAlreadyCommittedHistory` | A client subscribing from sequence 0 receives all already-committed records, in order — the same historical-replay guarantee as the brpc-based service, over real gRPC. |
| `EachSubscriberHasAnIndependentCursorFromItsOwnRequestedSequenceNumber` | Two concurrent gRPC subscribers, one from the beginning and one from the middle, each receive exactly what they asked for, independently. |

**A real shutdown-hang bug this test caught.** Both cases initially
hung — not crashed — for the full length of whatever deadline
`grpc::Server::Shutdown()` was given, every run. Root cause: unlike
`GrpcOutputTransport::stop()` (`gateway/output/`), which wakes every
in-flight `Subscribe()` call *itself* (`SessionRegistry::closeAll()`)
before calling `Shutdown()`, `RelayGrpcServiceImpl::Subscribe`'s tailing
loop had no such mechanism — its only way to learn "please stop" was
`ServerContext::IsCancelled()`, which nothing ever set except
`Shutdown()`'s own deadline-expiry cancellation. Calling `Shutdown()`
with no deadline (the default is infinite) meant it waited forever on
a subscriber sitting idle at the tip of the journal, waiting for a
record that would never come — exactly the scenario a real relay
serves most of the time. Fixed with the same fast-wake pattern
`GrpcOutputTransport` already uses: `RelayGrpcServiceImpl::requestStop()`
sets an `std::atomic<bool>` the tailing loop already re-checks every
poll (5ms), called right before `Shutdown()` in both the test and
`run_relay_gateway.cpp`; `Shutdown()`'s own bounded deadline (5s) is
now just a backstop, not the primary mechanism — the same "belt and
suspenders" relationship `gateway/output/README.md` documents for its
own `closeAll()`/`Shutdown()` pair. Cut this test's own runtime from
~10s to ~0.2s; verified against 20 consecutive clean runs after the
fix.

## Seeing it in action

```sh
./build/debug/examples/counter/counter_node \
  --peer=127.0.0.1:8100:0 --peers=127.0.0.1:8100:0 --data_dir=/tmp/counter-node-0
./build/debug/gateway/relay/sequencer_relay \
  --data_dir=/tmp/counter-node-0 --listen_port=8500 --grpc_listen_port=8501
```

Any brpc client speaking `RelayService` (`proto/relay.proto`) can
subscribe on `--listen_port` — this repository's own
`sequencer::relay::RelaySubscribeClient`
(`include/sequencer/relay/subscribe_client.hpp`) is the reference
implementation; `tests/relay_gateway_test.cpp`'s `CollectingClient`
shows the exact usage pattern, including decoding the raw bytes it
receives back into a `journal::RecordView`. Any real gRPC client can
subscribe on `--grpc_listen_port` instead — reflection is enabled, so
`grpcurl` needs no `.proto` file of its own:

```sh
grpcurl -plaintext -d '{"from_sequence_number": 0}' 127.0.0.1:8501 \
  sequencer.gateway.relay.grpc_proto.RelayService/Subscribe
```

`raw_record` in each printed message is the same byte-identical,
base64-encoded journal record `RelaySubscribeClient` receives —
decode it and feed it to `journal::RecordView` exactly as
`tests/relay_grpc_test.cpp`'s `TestGrpcRelayClient::readOne` does, for
a language that has a gRPC client but no brpc one.
