# examples/counter/

The one example in this repository (specification.md
[§10](../../docs/specification.md#10-the-counter-example)): a running
total. Input is an 8-byte signed delta; state is the total; the one
output — also the designated output, since the submitting client is its
only interested party — is the new total after applying the delta.

At this phase (§15 items 3–4) that means the state machine, `node_main`
(proven with a **real three-node raft group**), and `replay_main` (the
determinism gate, proven in CI on every push) — the input codec, output
codec, gateways, and load generator that make up the rest of §10's full
layout land in later phases.

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

## Seeing it in action

Run a single node directly (it needs `--peer` and `--data_dir`; `--peers`
defaults to itself, forming a trivial one-node group):

```sh
./build/debug/examples/counter/counter_node \
  --peer=127.0.0.1:8100:0 --data_dir=/tmp/counter-node-0
```

Once that node (or any run of it) has proposed a few deltas, its
data directory holds a real journal — inspect it with `tools/dumper`,
or certify it with this example's own replay binary:

```sh
./build/debug/tools/dumper/dumper --data_dir=/tmp/counter-node-0
./build/debug/examples/counter/counter_replay --data_dir=/tmp/counter-node-0
```

See `tools/replay/README.md` and `tools/dumper/README.md` for what
each prints and what their flags do.

`three_node_smoke_test.cpp` is the fuller demonstration — three real
processes, a real election, real `Propose` RPCs — and is the best place
to read for the exact flags a multi-node local deployment needs
(`--group`, `--peer`, `--peers`, `--election_timeout_ms`).

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
