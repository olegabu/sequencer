# gateway/

The three gateways are the only components a client ever talks to
(specification.md [§3.3](../docs/specification.md#33-gateways)): nodes
are never addressed directly in a real deployment. Each has its own
README with the detail; this file is about what they have in common,
because they converged on the same shape for the same measured reason.

| component | direction | what it does |
|---|---|---|
| [`input/`](input/README.md) | client → group | accept a request, run the codec, propose to the leader, answer the client |
| [`output/`](output/README.md) | group → client | tail the journal, run the codec, disseminate to subscribers |
| [`relay/`](relay/README.md) | group → remote reader | tail a colocated replica's journal, re-serve it verbatim over the network |

## The one lesson all three learned

Every gateway started out doing **one wire operation per record**, and
in every case that was the thing that limited it — not the codec, not
consensus, not the application code. Profiling each of them under real
load produced the same picture: no application symbol anywhere near
the top, and a flat profile of socket syscalls, kernel spinlocks and
thread wakeups.

So all three now batch, under one rule:

> **Gather whatever is available right now, send once, never delay a
> send to wait for more.**

That rule is what makes batching free. An idle gateway sends batches of
one and behaves exactly as it did before; a busy one batches because
work genuinely accumulated while the last operation was in flight. No
timers, no artificial windows. This was tested directly and the
artificial version lost: the output gateway briefly had a
`--batch_window_us` that delayed a flush to build bigger batches, and
at 200 µs it bought ~100 µs of p50 while doubling p99.9. It was
removed.

| gateway | what one operation used to be | measured |
|---|---|---|
| relay | one journal record per gRPC `Write()` | a 7.3 s stall at 100k → ~1.3 ms |
| output | one record per transport write | ~4.2-4.9 ms → ~890 µs p50 at 100k |
| input | one client request per `Propose` RPC | ~1.34 ms → ~1.03 ms p50 at 100k |

## Where they deliberately differ

Batching is the shared answer; the structure around it is not, because
the three have genuinely different shapes.

**`output/` fans one record out to many subscribers, with no reply.**
Its hot path was a *cross-thread hand-off*: one tailing thread pushed
every record into per-session queues that transport threads later
drained. That is now a single-writer `BroadcastRing` with a private
cursor per subscriber, each draining straight to its own socket — the
producer publishes once no matter how many subscribers exist, and
nobody hands anything to anybody. Fire-and-forget writes are also why
one batch in flight costs it nothing.

**`input/` turns one client request into one proposal and must wait
for an answer.** That round trip is what makes it different: batching
the wire calls helps, but *serializing* them does not. The first
version allowed a single `ProposeBatch` in flight and measured 133 ms
p50 with throughput down to 75k — it had replaced full pipelining
across every in-flight client request with one outstanding
request-response pair. It now allows several modest batches at once
(`--max_batch_size`, `--max_inflight_batches`), because a batch's
latency is its slowest member and its throughput ceiling is
`batches × size / RTT`.

**`relay/` re-serves journal bytes unmodified, so it needs no ring at
all.** Its subscribers pull the mmap'd journal directly through
private cursors. `output/` cannot do that — it must *decode* each
record to route it (`Fanout::toSession` exists precisely so a codec
can address one session), so its ring holds post-codec bytes and is
the journal's stand-in.

## What profiling was worth, and what guessing was worth

Both gateway rewrites came out of `perf record -F 999 -g` against a
live fleet. Guessing did not do nearly as well, and the record is
worth keeping:

- **Reverted after measuring worse.** `output/`'s gRPC transport was
  rewritten onto `grpc::ServerWriteReactor`, copying a rewrite that
  had been a large win in `relay/`. It measured slightly *worse* and
  was reverted. The relay's win came from unblocking a thread that was
  both producer and writer; the output gateway's producer was already
  decoupled, so the problem never existed there.
- **Three hypotheses about `input/`, all wrong.** A per-request
  `brpc::Channel` construction (real bug, worth ~30 µs); the
  synchronous propose blocking a worker (rewritten async, measured
  flat); worker concurrency (~10%, and more is not better). Only the
  profile pointed at the actual answer.
- **A one-run result that wasn't.** One measurement of 891 µs looked
  like a 30% win from a concurrency setting; three repeats put it at
  1192 µs. Single runs near a knee are noise.

The async rewrite in the second bullet was kept even though it
measured flat, for a reason that is not throughput: a handler that
parks a worker waiting for the node cannot batch, because the worker
is the thing that would have to wait. It is a prerequisite, not a win.

## Admission control: the in-flight batch bound

`input/`'s proposer holds at most `--max_inflight_batches` batches on
the wire at once (default 16, `src/node_proposer.hpp`). A proposal
arriving when every slot is busy waits for one to come back — it is
*deferred*, and `input_gateway_proposals_deferred` counts it.

**Deferral is the mechanism working, not a queue to tune away.** That
sentence is here because the opposite was assumed first, and measuring
proved it wrong.

Instrumenting the path showed deferral climbing steeply with load —
about 5% of proposals at 100k req/s, about 80% at 300k — while the
gateway's own queue delay grew 25× (9 µs → 748 µs) and the leader's
apply wait grew only 1.6× (524 µs → 1037 µs). The gateway was
increasingly waiting on itself. The obvious reading was that the bound
was the ceiling, so the obvious fix was to raise it.

Raising it to 64 made **every** rate worse and moved the knee *down*,
from ~360k to ~330k. Lowering it did the opposite. Five input
gateways, one leader, end-to-end p50, and where the cluster stops
keeping up:

| slots | 100k | 300k | 350k | 400k | 450k |
|---|---|---|---|---|---|
| 4 | 803 | 1509 | **1654** | **2268** | **3204** |
| 8 | **680** | **1268** | 1568 | collapsed | — |
| 16 (default) | 683 | 1324 | 1750 | collapsed | — |
| 64 | 687 | 2070 | collapsed | — | — |

The reason is that **the bound is per gateway, but the leader feels
the sum.** Five gateways at 16 apiece put 80 concurrent batches on a
leader that does its best work near 16–20, and the leader's apply wait
degrades under that pressure — at 300k it measured 822 µs at 4 slots,
840 µs at 16, and 1374 µs at 64. Removing the backpressure does not
remove the queue; it moves the queue onto the leader, where it costs
more. Throttling at the gateway is what keeps the leader efficient.

### Tuning it

Start from the product, not the per-gateway number:

    slots per gateway  ≈  16 to 20  /  number of gateways feeding one leader

floored at 1. One gateway keeps the default of 16. Five gateways want
about 4, which is the only setting in the table above that carries
450k — roughly 25% past what the default reaches.

It is a trade, not a free win. Fewer slots means more proposals wait
for one, which costs at low rates: 4 slots measures 803 µs at 100k
against 680 µs at 8. **Tune for the load you must survive, not the
load you usually see** — the low-rate penalty is ~120 µs, while the
high-rate penalty for guessing wrong is the cluster falling over.

Batch size (`--max_batch_size`, default 64) is the other half and is
rarely the binding one: batches grow on their own as load rises (1.0,
2.0, 3.0, 4.6, 6.0 across 100k→400k at 4 slots) because a completing
batch immediately takes whatever queued while it was out. Raising the
cap to 256 measured clearly worse (1714 µs against 1009 µs at 100k),
since a batch's latency is its slowest member.

### Watching it

Both sides publish bvars, which brpc already exposes on each process's
`/vars` page — no wiring, no agent:

| bvar | where | says |
|---|---|---|
| `input_gateway_proposals_deferred` | gateway | admission control engaging |
| `input_gateway_batch_queue_delay_us` | gateway | time waiting for a slot |
| `input_gateway_batch_size` | gateway | whether batches are grouping |
| `input_gateway_queue_depth{,_max}` | gateway | how deep the wait queue gets |
| `node_propose_batch_apply_wait_us` | node | time inside the raft group |
| `node_propose_batch_inputs` | node | batch size as the leader sees it |
| `journal_fill_percent` | node | how close the journal is to full |

The first and the fifth are the pair that matters: together they split
a request's life into *waiting for a slot* and *waiting for raft*,
which end-to-end latency alone cannot separate. If queue delay is
rising while apply wait is flat, the gateway is throttling — and given
the table above, that is usually correct behaviour rather than the
problem.

Two traps worth knowing, both hit while producing the numbers above.
`bvar::IntRecorder` publishes a **lifetime** average, so under a rising
sweep it reports the history rather than the rate being measured — the
batch-size bvars are windowed for that reason. And bvar's windowed
statistics decay to zero when a bvar goes idle, so a scrape that lands
after the load stops reports 0 for everything, which is
indistinguishable from "nothing happened".

## Testing

Each component's README lists its own cases. Two are worth knowing
about from here because they cross component boundaries:

- `output/tests/output_gateway_test.cpp`'s
  `OneGatewayServesTwoTransportsFromOneJournalTail` — one gateway
  serving brpc and WebSocket subscribers at once. It found a real bug:
  the WebSocket transport was sending *text* frames, which RFC 6455
  requires be valid UTF-8, so Beast refused the write and the
  best-effort fanout dropped it silently.
- `input/tests/input_gateway_test.cpp`'s
  `ConcurrentSubmitsEachGetTheirOwnSequenceNumber` — batched results
  are positional, so this is where a mis-ordered or mis-sized batch
  would hand a client someone else's sequence number.

The end-to-end path through all of it is
`examples/counter/tests/end_to_end_test.cpp`: four real processes, a
real WebSocket client, and a real submitting `brpc::Channel`.
