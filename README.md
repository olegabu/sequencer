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

## Scope notes

Performance measurement — load-generation rigs, tuning sweeps, published
throughput and latency tables — is out of scope for this repository and
lives in a separate benchmarking repository, built against this one. The
first real application, a financial ledger, is being developed in its
own repository against the plug points defined here, not inside this
one.

## Development

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
`vcpkg` on first configure — nothing to install by hand. Today that
manifest lists only `gtest` and `benchmark` (all `journal/` currently
needs); `braft`, `brpc`, `protobuf`, and `boost-beast` join it once
`node/` and `gateway/` land, per `docs/specification.md` §9.

### Build

Configure and build via the CMake presets in `CMakePresets.json`
(`debug`, `release`, and `tsan` — the last builds every test under
ThreadSanitizer, for the journal's lock-free reader/writer protocol):

```sh
cmake --preset debug
cmake --build --preset debug
```

The first configure resolves and builds every `vcpkg.json` dependency
from source, which is slow (minutes, for the current gtest/benchmark-only
manifest; expect considerably longer once braft/brpc join it) — later
configures reuse vcpkg's cache and are fast.

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

