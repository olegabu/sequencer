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
[raft-tests](https://github.com/opensequencer/raft-tests) — a sibling repo
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

## The four round trips, and which this measures

specification.md §3's diagram (redrawn this same phase — see its
own commit) shows several genuinely different things a "how fast is
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
   the same machine as the client** — `relay_observer.hpp`'s
   `RelayObserver`. A background real-gRPC `RelayService` subscriber,
   colocated with the load generator itself (not a separate process,
   and not a gateway at all — no codec, no client protocol
   translation, just a `grpc::ClientReader<Record>` loop, exactly
   `relay_grpc_test.cpp`'s own `TestGrpcRelayClient`), correlating
   arrivals against this harness's own send-time table by journal
   sequence number — entirely within the client box's one clock, so
   the result is directly, safely comparable to (1) with no
   cross-machine clock-sync assumption. The delta between (3) and (1)
   is specifically how much longer dissemination to a remote,
   relay-fed consumer takes beyond the synchronous ack path — a number
   none of raft-tests' other three products have any equivalent of,
   since none of them have a relay-gateway concept.

   Opt in with `--relay_grpc_addr` (`examples/counter/load_generator_main.cpp`'s
   flag — empty, the default, disables it and costs nothing). Prints a
   second, separately-labeled summary (`relay_p50_us`, not `p50` —
   deliberately, so `raft-tests/sweep/sweep.sh`'s whitespace-anchored
   grep can never mistake one summary for the other regardless of
   print order) after the main one.

   **Thread synchronization, precisely, since this is the one place in
   this harness two independent threads race on purpose:** the journal
   sequence number a request was assigned isn't known until its
   response arrives — well after the request was actually sent — so
   `LoadGeneratorRequester::send()`'s `sendTimeUs` parameter exists
   specifically to carry the *original* reference send time forward to
   that point (see that method's own comment). Meanwhile the relay can
   — and regularly does — deliver a record *before* the synchronous ack
   finishes its own extra hop back through the input gateway, so
   `RelayObserver::recordSend()` (called when the ack's journal
   sequence number becomes known) and the subscriber thread's own read
   of the same slot genuinely race, not just in theory. Resolved with
   a lock-free ring buffer, one send-time slot and one tag slot per
   sequence number (`recordSend()` publishes with a release store on
   the tag; the subscriber's matching acquire load on the same tag
   guarantees it observes a consistent send time, not a stale one, per
   the standard release/acquire single-writer publish pattern), plus a
   bounded wait-and-retry (spin briefly, then back off, capped at
   100ms of wall-clock time — long enough to resolve a race against a
   genuinely slow-tail ack, not just a fast one, since dropping those
   early would bias percentiles by disproportionately excluding
   exactly the slowest, most interesting samples) rather than an
   immediate skip on the first check. The timestamp actually recorded
   is captured the instant the record arrives, before any of that
   waiting — so however long resolving the race takes never leaks into
   the reported latency itself. Sized correctly (`--relay_ring_capacity`,
   or the generous default derived from `--rate`/`--thread_num` and the
   warmup/measure/drain window), no slot is ever reused within one run
   at all, so there is no wraparound to reason about, only the
   ring-too-small case the tag check guards against defensively.

   One more case the wait-and-retry above must *not* apply to:
   `--relay_from_sequence_number` defaults to 0 (from the beginning of
   the journal), so against any journal with real prior history —
   every run on a fleet after the first one — the relay delivers a
   long run of records that predate this process's own requests
   entirely and can never resolve, no matter how long `waitForTag`
   waits. Reproduced directly: a mere 110 such records were enough to
   make an entire run look completely broken
   (`relay_completed=0`) before this was handled. `recordSend()`
   tracks the lowest journal sequence number it has ever been called
   with; `subscribeLoop()` skips `waitForTag` immediately for anything
   below that floor (`relay_skipped_historical` in the summary),
   rather than paying up to 100ms per backlog record on something that
   was never going to match.

4. **Submission to receipt via an output gateway** — the fourth round
   trip, and the one place this repository's own architecture offers a
   *choice* the relay doesn't: an output gateway applies an
   `OutputCodec` and disseminates over one of three transports
   (`gateway/output/`'s `BrpcOutputTransport`, `GrpcOutputTransport`, or
   `WebSocketOutputTransport`), so there are three genuinely different
   numbers here, not one. `sequence_correlator.hpp`'s
   `SequenceCorrelator` is (whichever transport is picked between)
   `RelayObserver`'s own correlation machinery, factored out and reused
   verbatim — the same ring buffer, the same reader-thread/correlator-
   pool split (see the wall of caution above this section for why that
   split exists at all; it applies here identically), the same
   historical-skip and bounded-wait-and-retry logic. What differs per
   transport is only *how a record arrives*:

   - **`grpc_output_observer.hpp`** (`GrpcOutputObserver`): a
     `grpc::ClientReader<OutputRecord>` loop, structurally identical to
     `RelayObserver::readLoop()`, on its own reader thread.
   - **`brpc_output_observer.hpp`** (`BrpcOutputObserver`): no reader
     thread of its own at all — `brpc::StreamInputHandler::
     on_received_messages()` already fires asynchronously on brpc's own
     I/O thread pool, so `deliver()` is called directly from that
     callback.
   - **`websocket_output_observer.hpp`** (`WebSocketOutputObserver`): a
     synchronous Boost.Beast `read()` loop, same shape as the gRPC
     reader, on its own thread.

   Sequence-number extraction is the one thing that can't be shared
   with `RelayObserver`: the relay hands back raw journal bytes (a
   fixed, app-agnostic binary format, parsed via
   `journal::RecordView::sequenceNumber()`), but an output gateway's
   payload is whatever its `OutputCodec` produced — JSON for counter,
   arbitrary bytes for any other application. `SequenceCorrelator`
   takes the extractor as a constructor argument instead
   (`std::function<std::optional<std::uint64_t>(const std::string&)>`)
   — `examples/counter/load_generator_main.cpp` supplies
   `extractJsonIntField(payload, "sequence_number")`, the same helper
   already used to parse the synchronous ack's own response body.

   Opt in with `--output_observer=grpc|brpc|websocket` plus
   `--output_gateway_addr` (empty `--output_observer`, the default,
   disables it and costs nothing) — independent of, and freely
   combinable with, `--relay_grpc_addr`: a single run can report all
   four numbers at once. Prints a third summary, namespaced per
   transport (`output_grpc_p50_us` / `output_brpc_p50_us` /
   `output_websocket_p50_us`), the same discipline `relay_p50_us`
   exists for.

## The FIX sender, and the rig-not-the-bottleneck criterion

`include/sequencer/bench/fix_requester.hpp` is a `LoadGeneratorRequester`
over the same session core the gateway's acceptor runs, in `Initiator`
role (specification.md §8.12). Replies are correlated by a private tag
(5000), not by order, because a session gateway delivers outputs in
journal order rather than request order.

A rig has to outrun the system it measures, or its numbers describe
itself. The criterion is **≥2× the highest rate the system is targeted
at, with zero drops.**

Measured on the development machine, release build, 2026-08-31, over
four isolated runs:

| offered | achieved per sender | drops |
|---|---|---|
| 200,000/s | **~77,000/s** (76.5k, 78.3k, 78.7k, 87.1k) | **0** |

A correction to the figure above: it was measured against a trivial
echo peer AND before `FixRequester` sent `ResetSeqNumFlag` on Logon.
Against a real gateway a single sender sustains **20,000/s with zero
drops** (see `gateway/fix/README.md`), so the per-sender constraint is
far less tight than this section first concluded.

**The criterion is met by the rig, not by one sender**, and the
distinction matters. The rig is not a single process: sweeps run from
five client boxes, each with a load generator beside its own gateway,
the offered rate split between them and latencies merged from every
client's raw histogram (`sweep/sweep-multi.sh`, `sweep/merge-hdr.py`).
Five senders at ~77k/s offer roughly **385k/s**, which clears 2× a 100k
sweep with room to spare.

The operational consequence: **a FIX sweep must be run multi-client.**
Driven from one box it would measure the rig, not the gateway — the
same trap that made an earlier sequencer "knee" turn out to be the
client rather than the cluster.

One reading of 187k/s was recorded and is discarded: it did not
reproduce, and it was taken while other copies of the benchmark were
running. Single readings near a limit are noise.

Reproduce with:

```sh
./build/release/gateway/fix/tests/fix_loadgen_loopback_test \
  --gtest_also_run_disabled_tests
```

Loopback is deliberate and limited. It isolates the sender's own cost
per message by removing the NIC, and it mirrors the real topology,
where the load generator and its gateway share a client box and only
the proposal crosses the network. It says nothing about gateway latency
on real hardware.
