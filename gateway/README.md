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
