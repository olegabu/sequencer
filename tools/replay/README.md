# tools/replay/

The sequencer's warranty (specification.md
[§11](../../docs/specification.md#11-determinism-certification)):
replay a recorded input sequence through a **fresh** `StateMachine`
instance and compare the resulting journal byte-for-byte against the
original. Any divergence is a bug — in the state machine (a
[§4.1](../../docs/specification.md#41-determinism-rules-binding-on-every-state-machine)
rule violated), the harness, or the journal protocol itself.

Because it needs a concrete state machine to replay against, this is a
**library**, invoked the same way `RunNode` is — an application links
it and exposes its own replay binary. `examples/counter/replay_main.cpp`
is nine lines showing the whole pattern.

## Testing

```sh
cmake --preset debug
cmake --build --preset debug
ctest --preset debug --output-on-failure
```

`tests/replay_check_test.cpp` drives the testable core (`runReplayCheck`,
in `src/replay_check.hpp` — separated from argv/gflags parsing exactly
as `node/`'s `NodeImpl` is separated from `RunNode`) directly, with a
small state machine defined locally rather than depending on
`examples/counter` (`tools/` links against nothing above it —
specification.md §9: "nothing depends on examples"):

| Case | Proves |
|---|---|
| `IdenticalStateMachineReplaysByteIdentical` | The straightforward case: record, replay through a fresh instance, get the same journal. |
| `DivergentStateMachineIsDetectedAndOutputDirPreserved` | Replay actually **catches** a real divergence (forced here by giving the replay instance a different starting state) — reports the first differing sequence number and leaves the replayed journal on disk for inspection, rather than cleaning it up. |
| `EmptyJournalReplaysTrivially` | Zero records is a valid, vacuously-passing case. |
| `ExplicitOutputDirIsUsedAndNotAutoRemoved` | An explicitly-provided output directory is the caller's, never auto-deleted — only the default temp directory is. |

`examples/counter/tests/replay_test.cpp` is the companion test that
matters more in practice: the same core, but with the **real**
`CounterStateMachine` — this is specification.md §11's "run it in
continuous integration on every example and application" gate, and it's
what `.github/workflows/ci.yml` runs on every push.

## Seeing it in action

```sh
# counter_replay is examples/counter's replay binary (replay_main.cpp
# linked against this library and CounterStateMachine).
./build/debug/examples/counter/counter_replay --data_dir=/path/to/a/node/data_dir
```

Prints `replay OK: N record(s) byte-identical` and exits 0 on success;
on divergence, prints the first differing sequence number, exits
nonzero, and — unlike the success path — leaves the replayed journal on
disk (path printed to stderr) so you can compare it against the
original with `tools/dumper`.

## Design notes

- `runReplayCheck` sizes the replay journal's `JournalOptions` from the
  *original* journal's own footprint (`journal.data`'s file size, the
  original's record count) rather than trusting some unrelated default
  — a large recorded journal shouldn't silently exhaust a hardcoded
  replay-time limit.
- Comparison is by `RecordView::rawBytes()` per sequence number — the
  same record layout §7.2's Merkle leaves hash over, so "byte-identical"
  here means exactly what "byte-identical" means for evidence, too.
- No `--replay_output_dir` given → a temp directory, removed on success,
  left in place (and named in the output) on any divergence — replay
  failures should hand you something to diff, not just a verdict.
