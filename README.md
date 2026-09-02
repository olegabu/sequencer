# The Sequencer

**The sequencer is a library that turns a deterministic state machine
into a fault-tolerant service with a verifiable history.**

It accepts opaque, client-signed inputs from many concurrent clients,
arranges them into a single replicated total order (Raft, via braft),
applies each to the hosted state machine exactly once — in order,
identically on every replica — and appends `{sequenceNumber, input,
outputs[]}`, each input with its full consequence set, as one atomic
record in a replayable journal that is the system's authoritative
output stream. Blocks of journal entries are Merkle-rooted and signed,
so origin (client signatures), order (dense sequence), outcome
(deterministic replay), and operator commitment (signed roots) are each
independently verifiable by any observer.

## Why

Systems whose correctness must be *demonstrable* — exchanges, ledgers,
clearing and settlement, betting markets — all reduce to the same core:
one total order of inputs, one deterministic interpretation, one
replayable record. Building that core well once (replication, a
lock-free journal, cryptographic evidence, honest tests) and plugging
business logic into it beats rebuilding it inside every product. The
guarantee offered is precise: *verifiable if published honestly*, where
the first provable lie is terminal — a signed receipt contradicting the
published journal is a self-contained proof of fraud. This buys strong
auditability at Raft latency, without a token, a validator set, or
Byzantine-consensus overhead.

Just as important for a financial system: deployed across cloud
availability zones, the replicated cluster is safe and **self-healing**.
A node or whole-zone loss triggers automatic leader re-election with no
committed message lost and no operator intervention — the system simply
continues. Honestly stated: re-election takes seconds, orders of
magnitude above the sub-millisecond latencies targeted elsewhere in this
design — yet far shorter than a classical failover to a backup site, and
with nobody's pager firing before service resumes.

## Contract

- **To applications:** implement three small interfaces —
  `StateMachine` (`apply(sequenceNumber, input, outputs)` plus snapshot
  hooks), `InputCodec`, `OutputCodec` — follow the determinism rules, and
  the sequencer makes your logic replicated, ordered, durable,
  disseminated, and provable.
- **To clients:** an acknowledged input is committed at its sequence
  number and will be deterministically processed; a signed proof binds
  the operator to that position forever; no acknowledgement means
  resubmit safely (idempotency absorbs it).

## Main components

| Component | What it is |
|---|---|
| `node/` | The harness: braft wiring (private, in `node/src/raft/`), the pinned apply thread, sequence-number minting, deferred acknowledgement. `RunNode(argv, yourStateMachine)` is a whole node. |
| `journal/` | The product: memory-mapped, lock-free, single-writer/many-reader log of inputs and outputs; readers tail without ever backpressuring the writer. |
| `gateway/` | Stateless edge chassis: `RunInputGateway` (brpc/REST/gRPC on one port), `RunOutputGateway`, and the symmetric `InputCodec`/`OutputCodec` interfaces applications implement — plus the **relay gateway**, a codec-free stock binary that carries a node's journal off its machine unmodified, so output gateways and the signing gateway never touch a node directly at scale. Run at least two instances of each type. |
| `evidence/` | Deterministic Merkle blocks over journal records, signed by the signing gateway — an ordinary journal reader, so a node itself does no operator cryptography. |
| `sdk/` | Reference client, per language: propose, verify proofs against your own bytes, reconcile against the journal, alarm on missing proofs — includes each language's journal reader, since verification requires reading the journal anyway. |
| `tools/` | `replay` (a library an application links to certify byte-identical replay — the signature test) and `dumper` (a standalone raw-record viewer). |
| `bench/load_generator/` | A reusable open/closed-loop, HDR-histogram load-testing harness — the piece an application-specific load generator (`examples/counter`'s included) supplies a request body and destination to. Feeds the sibling [raft-tests](https://github.com/olegabu/raft-tests) repo, this repository's actual benchmarking harness. |
| `examples/counter/` | The one example: a running total, its input and output codecs, node/gateway executables, and a load generator — the complete plug-point pattern at the smallest useful scale. |

## Design document

Everything normative — interfaces, the journal's byte format and the
reasoning behind it, determinism rules, the evidence layer, gateway
contracts, tuning defaults, deployment guidance, the acceptance
checklist, and the implementation order — lives in one self-contained
specification:

**[`docs/specification.md`](./docs/specification.md)** — read it before
writing code; it is the single source of truth and is written to drive
the entire development.

**[`docs/testing.md`](./docs/testing.md)** — every end-to-end test and
every specification.md §14 acceptance-checklist test in the
repository, described in full: what's real, what's asserted, and where
it lives.

## Scope notes

Performance measurement — load-generation rigs, tuning sweeps, published
throughput and latency tables — is out of scope for this repository and
lives in a separate benchmarking repository, built against this one. The
first real application, a financial ledger, is being developed in its
own repository against the plug points defined here, not inside this
one.

## Development

Once the prerequisites below are installed, `make` builds and `make
test` builds and runs the full suite. `Makefile` is a thin wrapper
around the `cmake`/`ctest` preset commands the Build and Test
subsections below document manually — see [Make targets](#make-targets)
for the full list.

### Prerequisites

Install and clone all of the following before attempting a build.
Component directories pull in more of this list incrementally as they
land (`journal/` needs only the first four items; `node/` is what
first requires braft/brpc/protobuf) — see `docs/specification.md` §15
for the implementation order.

**OS:** Ubuntu 20.04 LTS (`focal`), x86_64. Nothing here is expected to
be Ubuntu-specific in principle, but this is the only platform this
repository is developed and tested against.

**Tools (install via `apt`):**

- `cmake` ≥ 3.24
- `ninja-build`
- `g++-10` — **not** the distribution's default `g++` (GCC 9), which
  predates `<span>`; the codebase requires C++20 and is compiled with
  `g++-10` explicitly (see `CMakePresets.json`)
- `git`

```sh
sudo apt update
sudo apt install -y cmake ninja-build g++-10 git
```

**vcpkg** (dependency manager, manifest mode via `vcpkg.json`) — clone
and bootstrap it yourself; it is not an `apt` package:

```sh
git clone https://github.com/microsoft/vcpkg.git ~/workspace/vcpkg
~/workspace/vcpkg/bootstrap-vcpkg.sh
```

Then export `VCPKG_ROOT` (add this to your shell profile so it persists
across sessions — `CMakePresets.json` reads it to locate the toolchain
file):

```sh
export VCPKG_ROOT="$HOME/workspace/vcpkg"
export PATH="$VCPKG_ROOT:$PATH"
```

**Libraries:** declared in `vcpkg.json` and fetched automatically by
`vcpkg` on first configure — nothing to install by hand. The manifest
now lists `gtest`, `benchmark`, `braft`, `brpc`, `boost-beast` (the
output side's WebSocket transport, `gateway/output/`'s
`WebSocketOutputTransport`, per §8.7), `grpc` (the output side's real
gRPC streaming transport and the relay's real gRPC service, both in
`gateway/`, per §8.7 — brpc speaks only *unary* gRPC, not streaming),
`openssl` (the cryptography library `evidence/`'s Merkle signing
and `sdk/`'s proof verification share, per §9.1), and `hdr-histogram`
(`bench/load_generator/`'s percentile tracking only) — the full set
`docs/specification.md` §9 anticipates for this repository, unless a
future phase's design calls for something new.

### Build

Configure and build via the CMake presets in `CMakePresets.json`
(`debug`, `release`, and `tsan` — the last builds every test under
ThreadSanitizer, for the journal's lock-free reader/writer protocol):

```sh
cmake --preset debug
cmake --build --preset debug
```

The first configure resolves and builds every `vcpkg.json` dependency
from source — braft and brpc alone can take the better part of an hour
on modest hardware — but vcpkg caches what it builds, so later
configures (including switching between the `debug`/`release`/`tsan`
presets, each of which gets its own `build/<preset>/` directory) are
fast. If a preset's own `build/<preset>/vcpkg_installed/` is deleted or
was never populated (for example, only `debug` has ever been built),
that preset pays the from-scratch cost independently, even though the
others already paid it.

For a Release build (enables link-time optimization per
`docs/specification.md` §9.1):

```sh
cmake --preset release
cmake --build --preset release
```

### Test

```sh
ctest --preset debug --output-on-failure
```

To specifically check the journal's cross-thread acquire/release
protocol (`docs/specification.md` §6.3) for data races rather than just
correctness:

```sh
cmake --preset tsan
cmake --build --preset tsan
ctest --preset tsan --output-on-failure
```

Micro-benchmarks (regression tripwires for hot paths — e.g. journal
append cost, per `docs/specification.md` §12's latency expectations) are
separate from `ctest` and run directly:

```sh
./build/debug/journal/benchmarks/journal_benchmark
```

`.github/workflows/ci.yml` runs the full `ctest --preset debug` suite —
including `docs/specification.md` §11's determinism-replay gate — on
every push and pull request against `main`.

### Make targets

`Makefile` wraps every command in Build and Test above. It's the only
Makefile in the repository — `find_package()` for
braft/brpc/protobuf/etc. lives only in the root `CMakeLists.txt`
(specification.md §9's dependency graph), so no subdirectory
configures standalone.

| Target | Equivalent to | Notes |
|---|---|---|
| `make` / `make build` | `cmake --preset debug && cmake --build --preset debug` | Default target; bare `make` builds, it doesn't print help |
| `make test` | `make build`, then `ctest --preset debug --output-on-failure` | |
| `make debug` / `make test-debug` | `make build`/`make test` with `PRESET=debug` | Same as the two above, spelled out explicitly |
| `make release` / `make test-release` | `... PRESET=release` | The LTO build (§9.1) |
| `make tsan` / `make test-tsan` | `... PRESET=tsan` | ThreadSanitizer — the journal's §6.3 cross-thread protocol |
| `make configure` | `cmake --preset $(PRESET)` | Configure only, no build |
| `make test TEST_FILTER=<regex>` | adds `-R "<regex>"` to the `ctest` call | Run only matching test names, e.g. `TEST_FILTER=RelayGateway` |
| `make demo` | `make debug`, then `./examples/counter/demo_http_websocket.sh` | The curl + websocat walkthrough |
| `make benchmark` | `make debug`, then `./build/debug/journal/benchmarks/journal_benchmark` | |
| `make clean` | `rm -rf build/debug build/release build/tsan` | `make clean PRESET=release` removes just that one preset's directory |
| `make distclean` | `make clean`, plus wiping vcpkg's `buildtrees`/`downloads`/`packages` | Forces every dependency to rebuild from source next configure — see the from-scratch cost noted in Build above |
| `make help` | — | Prints this list |

Any target that defaults to the `debug` preset accepts `PRESET=release`
or `PRESET=tsan` to redirect it, e.g. `make build PRESET=tsan` is the
same as `make tsan`.

## Deployment

`docs/specification.md` §13's recommendation: **one node per
availability zone, across three zones** — survives the loss of any
single zone at the roughly one-millisecond commit floor §12 describes.
Placement of the majority, not the total node count, is the lever that
trades latency against resilience; a single-zone-optimized deployment
(a majority of nodes co-located in one zone) lowers commit latency
substantially but means that zone's loss halts the system entirely —
**a deployment choosing this must document that all-or-nothing exposure
explicitly**, per §13 and §14 item 5, rather than presenting it as the
general recommendation. Run at least two instances of every gateway
type (§3.2).

Failover is automatic and lossless: on leader loss the cluster
re-elects without operator intervention, no committed record is lost,
and clients recover by resubmitting unacknowledged in-flight inputs
against the new leader. Latency and throughput measurement themselves
are out of this repository's scope (§9, §12) and live in a separate
benchmarking repository built against this one — what this repository
verifies directly is the *functional* claim (no gaps, no divergence,
service continues), via the drills below.

## Acceptance checklist

`docs/specification.md` §14's five items, and where each is verified in
this repository:

| # | Checklist item | Verified by |
|---|---|---|
| 1 | Replay is byte-identical on a fresh build | `examples/counter/tests/replay_test.cpp`, run on every push by `.github/workflows/ci.yml` |
| 2 | A kill-leader-under-load drill shows no journal gaps, no divergence, client recovery, and the new leader's journal continuing densely | `examples/counter/tests/kill_leader_drill_test.cpp`'s `KillLeaderUnderLoadShowsNoGapsNoDivergenceAndContinuedCommits` |
| 3 | A client verifies a proof against retained bytes; proof reconstruction from the published journal alone succeeds; the proof-timeout alarm fires when the signing gateway is stalled | `sdk/cpp/tests/proof_verifier_test.cpp` (retained bytes); `sdk/cpp/tests/acceptance_drill_test.cpp` (journal-alone reconstruction and the stalled-alarm drill) |
| 4 | A restarted output gateway's dissemination is identical to an uninterrupted run | `gateway/output/tests/restart_drill_test.cpp` |
| 5 | A zone-loss drill shows continued operation; a single-zone-optimized deployment documents its exposure | `examples/counter/tests/kill_leader_drill_test.cpp`'s `ZoneLossKillingAFollowerLeavesTheSystemOperatingOnTheRemainingTwoZones`, and the Deployment section above |

Every drill test above runs a real, multi-process cluster (or, for item
3's signing-gateway drill, a real signing gateway against a
deliberately incomplete journal) and injects the actual fault —
`SIGKILL` for process loss, a permanently-incomplete block for a
stalled signing gateway — rather than mocking the failure mode.

**[`docs/testing.md`](./docs/testing.md)** covers this in full — every
checklist item's test cases described in detail, plus every end-to-end
test in the repository (multi-process and single-process-real-transport
alike) that isn't a plain per-file unit test already covered in its own
component's README.

