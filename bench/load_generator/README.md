# bench/load_generator/

A reusable open/closed-loop load-testing harness — HDR-histogram
percentiles, a bounded warmup window before measurement starts, drop
tracking under sustained overload, a schedule-lag histogram auditing
the rig itself. Everything an application-specific load generator
needs *except* what to send and where — that split is deliberate, and
mirrors `InputCodec`/`OutputCodec` splitting meaning from mechanism
elsewhere in this repository (specification.md §8.5): an application
implements `LoadGeneratorRequester`; this header does the rest.

## Why this exists, and what it is not

This is **not** this repository's actual benchmarking harness. That is
[raft-tests](https://github.com/olegabu/raft-tests) — a sibling repo
comparing this project's own raft implementation choice (braft)
against openraft and aeron, with the terraform/EC2 deployment,
multi-rate sweeps, and knee-curve chart generation that real
benchmarking needs. `examples/counter/load_generator_main.cpp` used to
be a much more primitive tool — no histogram, no warmup/measure
separation, just a count-based run — explicitly documented as "a smoke
test and a rough sanity check, not a substitute for the benchmarking
repository." This header is what closes most of that gap: it exists so
that `examples/counter` (and any future sequencer application) has a
harness sophisticated enough to plug directly into raft-tests'
`sweep/sweep.sh`, rather than reimplementing one per application the
way `raft-tests/braft/client.cpp` had to for bare braft.

Concretely, `raft-tests/sequencer/Makefile` (in the sibling repo) runs
this same load generator against sequencer's own binaries, on the same
3-node-plus-client EC2 fleet every other product in that repo uses —
see that Makefile's own comments for exactly how, and that repo's
README for what "Adding a new raft product" expects from a load
generator's reported shape.

## Why not `sdk/cpp/`

`sdk/cpp/` is deliberately header-only and dependency-light — only
`journal/` and `evidence/`, no brpc (see that library's own
`propose_client.hpp`, whose whole design is a caller-supplied
transport callback specifically so `sdk/` never needs to depend on
brpc or any particular gateway's proto). A load generator inherently
needs brpc (to drive real RPCs), gflags/glog, and an HDR histogram
library — a heavier, different dependency shape that has no business
being forced into `sdk/`'s contract. `bench/` is a new top-level
sibling to `sdk/`, `tools/`, and `evidence/` for exactly this: useful,
reusable, but neither part of the core library nor light enough for
`sdk/`.

## Using it

```cpp
class MyRequester : public sequencer::bench::LoadGeneratorRequester {
 public:
  void send(std::int64_t sequence, std::function<void(bool ok)> onDone) override {
    // Build and send one request tagged `sequence`; call onDone(ok)
    // exactly once, synchronously or later from any thread (e.g. a
    // brpc callback — required for open mode, since scheduling would
    // otherwise block on each reply).
  }
};

MyRequester requester(...);
sequencer::bench::LoadGeneratorConfig config;
config.mode = "open";
config.rate = 100000;
// ... see load_generator.hpp's LoadGeneratorConfig for every field.
sequencer::bench::LoadGenerator(requester, config).run();
```

See `examples/counter/load_generator_main.cpp` for a complete,
working instance: `SubmitRequester` builds `{"delta": N}` JSON and
submits it over a real `brpc::Channel` to an input gateway's
`SubmitService`.

## The three round trips, and which this measures

specification.md §3's diagram (redrawn this same phase — see its
own commit) shows three genuinely different things a "how fast is
this" question could mean for this repository specifically, unlike a
bare-braft product with no gateway or relay tier at all:

1. **Submission to synchronous receipt** — client → input gateway →
   node → (raft commit, apply, journal append) → node → input gateway
   → client. What this harness measures today, and the one directly
   comparable to `raft-tests/braft/client.cpp`'s own number (one hop
   longer, through the input gateway — specification.md §3.3 is
   explicit that this hop belongs in front of every real submission,
   never bypassed except for local testing, so it belongs in the
   number).
2. **Submission to durable-in-the-journal** — strictly *shorter* than
   (1): specification.md §5.1's apply-thread ordering
   (`journal.append()` happens before `acknowledge{}`, on the same
   thread, for every commit) makes the synchronous receipt latency
   already a safe, tight upper bound on this one, with no separate
   instrumentation needed to get a good number. Isolating it exactly
   (splitting out how much of (1) is commit-and-append versus the
   ack's own transit back to the client) is node-internal
   instrumentation — the node's own timestamps around `journal.append`,
   or braft's own append-latency tracer (`--raft_trace_append_entry_latency`,
   already available on `counter_node` for free, the same as every
   other `raft_*` flag `raft-tests/braft/run_server.sh` sets, since
   both link the same braft library) — not a new client-observed round
   trip, and not built here.
3. **Submission to receipt via the relay gateway, over the network, on
   the same machine as the client** — not built here either. See the
   design note this same conversation produced for the shape it would
   take: a background real-gRPC `RelayService` subscriber, colocated
   with the load generator itself (not a separate process, and not a
   gateway at all — no codec, no client protocol translation, just a
   `grpc::ClientReader<Record>` loop, exactly `relay_grpc_test.cpp`'s
   own `TestGrpcRelayClient`), correlating arrivals against this
   harness's own send-time table by sequence number — entirely within
   the client box's one clock, so the result is directly, safely
   comparable to (1) with no cross-machine clock-sync assumption. The
   delta between (3) and (1) is specifically how much longer dissemination
   to a remote, relay-fed consumer takes beyond the synchronous ack path
   — a number none of raft-tests' other three products have any
   equivalent of, since none of them have a relay-gateway concept.
