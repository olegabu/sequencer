# node/

The node is the harness that turns a `StateMachine` into a replica: one
brpc server wearing three hats (`Propose`, braft's own replication RPCs,
and brpc's monitoring pages — [specification, §3.1](../docs/specification.md#31-brpc-server-topology)),
one Raft group (via [braft](https://github.com/apache/braft)) providing
the total order, and exactly one pinned apply thread that mints each
committed input's sequence number, calls the state machine's `apply()`,
appends the result to the journal, and only *then* acknowledges the
client — "journal append precedes acknowledgement" is not a suggestion,
it's the whole trust model. Full details — the threading model, the
synchronous `Propose` reply, and why the apply thread busy-spins
unconditionally — are in
**[the specification, §5](../docs/specification.md#5-the-node-harness)**.

`node/` depends on `journal/` publicly; braft and brpc are private to
`node/src/raft/` — no braft type appears in `node/include/` (§9).

## What's here (and what isn't yet)

This is specification.md §15 item 2: `RunNode`, the committed-entry
ring, the pinned apply thread, sequence-number minting, and deferred
acknowledgement — proven end to end on a single replica. A three-node
smoke test (leader failover, replication across real peers) is item 3
paired with the counter example's state machine, which doesn't exist in
this repository yet — so there's no example `StateMachine` to point you
at below, only `node/tests/`' own trivial one.

## Testing

```sh
cmake --preset debug
cmake --build --preset debug
ctest --preset debug --output-on-failure
```

| File | Covers |
|---|---|
| `committed_entry_ring_test.cpp` | The lock-free single-producer/single-consumer ring between braft's callback thread and the apply thread (§5.4) — round trips, FIFO order across wraparound, an oversized-input guard, and a concurrent producer/consumer stress test. Entirely braft-agnostic: no braft or brpc symbol is linked into this binary. |
| `apply_loop_test.cpp` | The pinned apply thread's core loop, driven by a synthetic producer: dense sequence-number minting, journal append order, and the completion callback — also braft-agnostic. |
| `node_integration_test.cpp` | A **real** single-node `NodeImpl`: real disk-backed raft log/meta/snapshot storage, a real brpc server on a real port, real `Propose` RPCs over a real `brpc::Channel`. Waits for the (trivial, one-node) group to self-elect leader, then verifies the response sequence numbers, designated outputs, and the resulting journal. |

The committed-entry ring and the apply loop are deliberately
braft-agnostic (see their header comments) so they're testable without
braft or brpc in the link at all — only `node_integration_test.cpp`
pays for a real consensus round trip, which is also why it's the
slowest file here (leader self-election plus a few real RPCs, a few
seconds rather than milliseconds).

## Seeing it in action

`node_integration_test.cpp` is the real demonstration — it stands up an
actual replica and drives it over the network exactly as a gateway
would. The shape of it:

```cpp
NodeConfig config;
config.groupId = "test-group";
config.peerId = "127.0.0.1:28931:0";      // this replica's own address
config.initialPeers = config.peerId;       // single-node group: itself
config.dataDir = "/tmp/some-dir";           // journal + raft log/meta/snapshot

NodeImpl node(config, std::make_unique<MyStateMachine>());
node.start();
// ... poll node.isLeader() ...

// Propose over a real brpc::Channel to "127.0.0.1:28931" (no ":idx" —
// that suffix is braft's PeerId format, not a brpc endpoint).
```

A real application never touches `NodeImpl` directly — it's `RunNode`
that a whole node binary calls, exactly as specification.md §4/§9
describe:

```cpp
int main(int argc, char** argv) {
  return sequencer::RunNode(argc, argv, std::make_unique<MyStateMachine>());
}
```

`RunNode` reads its configuration from flags (defined in
`src/run_node.cpp`): `--peer` and `--data_dir` are required; `--group`,
`--peers` (the initial peer list, consulted only when bootstrapping a
brand-new group), `--election_timeout_ms`, and `--apply_thread_cpu`
(§5.3's recommended core-pinning, off by default for portability) all
have defaults.

## A build note worth knowing

The pinned vcpkg baseline's `glog` (0.7.1) and `braft`/`brpc` don't
compile together as installed — glog 0.7.1 restructured the internal
namespace its own `CHECK`/`DCHECK` macro expansions resolve against, and
braft/brpc's headers, written against the older layout, fail with
errors like `'LogMessageFatal' is not a member of 'google'` deep inside
third-party headers you never touch yourself. `vcpkg.json` pins `glog`
back to `0.6.0` via an `overrides` entry to route around it. If a future
`vcpkg.json` update ever needs to move the baseline forward, re-check
this pin first — it may no longer be necessary, or a newer braft/brpc
may need it lifted differently.
