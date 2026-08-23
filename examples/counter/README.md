# examples/counter/

The one example in this repository (specification.md
[§10](../../docs/specification.md#10-the-counter-example)): a running
total. Input is an 8-byte signed delta; state is the total; the one
output — also the designated output, since the submitting client is its
only interested party — is the new total after applying the delta.

As of §15 item 6, the full §10 layout exists: the state machine,
`node_main` (proven with a **real three-node raft group**),
`replay_main` (the determinism gate, proven in CI on every push), the
input and output codecs, both gateway mains, an open-loop load
generator, and an end-to-end test that drives all three processes
together over real sockets — the whole path from a JSON `Submit` on the
input gateway through the node's raft group to a WebSocket broadcast on
the output gateway.

## The codecs

`CounterInputCodec` (`counter_input_codec.hpp/.cpp`) parses a submitted
body as `{"delta": N}` into the 8-byte signed input the state machine
expects, rejecting anything that isn't valid JSON or is missing the
field — the gateway never calls `Propose` for a request the codec
rejects. Its `toOutput` builds the synchronous response the submitting
client sees: `{"sequence_number":N,"total":M}`. Since the protocol is
stateless (a delta doesn't depend on anything about the connection that
sent it), `onDisconnect` always returns `std::nullopt`.

`CounterOutputCodec` (`counter_output_codec.hpp/.cpp`) reads a
committed record's designated output (the new total, or `0` if the
record has none) and broadcasts the same
`{"sequence_number":N,"total":M}` JSON to every subscriber of the
`"totals"` topic — the one topic this example has, since every client
cares about the same running total.

Both codecs share `json_util.hpp`'s minimal hand-rolled field
extraction and JSON building — specification.md's dependency list has
no JSON library, and this example's wire format is one integer field
each way, not worth pulling one in for.

## The output transport: WebSocket, not brpc Streaming

`gateway/output/`'s chassis is transport-agnostic
(`sequencer::OutputTransport`, see
[gateway/output/README.md](../../gateway/output/README.md)); this
example plugs in `WebSocketTransport` (`websocket_transport.hpp/.cpp`)
instead of the chassis's built-in brpc-Streaming `BrpcStreamTransport`,
so a browser can subscribe directly with no brpc client of its own —
specification.md §8.7 calls out WebSocket as the other
zero-additional-dependency choice, this time for browser-facing
consumers, and Boost.Beast as the concrete library, scoped to just this
target (`find_package(boost_beast CONFIG REQUIRED)` lives in this
directory's `CMakeLists.txt`, not the top-level one — nothing else in
the repository needs it).

The implementation is one `boost::asio::io_context` running on a single
dedicated thread, with every `websocket::stream` touched only from that
thread — Beast's own documentation states plainly that a stream's
"shared objects" are unsafe to touch concurrently, so `broadcast`/
`toSession` (called from the tailing loop's thread) never write to a
socket directly; they hand the message to the io-thread via
`net::post()`, and a per-connection write queue (respecting Beast's
"one outstanding write at a time" rule) does the actual send.

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
| `counter_codec_test.cpp` | Both codecs in isolation, no gateway or process involved: `CounterInputCodec` parsing valid/negative/whitespace-tolerant/missing-field/non-JSON bodies and building its response JSON; `CounterOutputCodec` broadcasting a record's total (and defaulting to `0` for a record with no outputs) to the `"totals"` topic of a recording `Fanout` test double. |
| `websocket_transport_test.cpp` | `WebSocketTransport` against a real synchronous Beast client: a connected client receives a broadcast, several broadcasts arrive in order, and broadcasting to a topic with no subscribers is a silent no-op. Stress-tested well beyond a single clean run while chasing the thread-safety redesign below. |
| `end_to_end_test.cpp` | The full pipeline, four real processes (`counter_node`, `counter_input_gateway`, `counter_output_gateway`, plus a test WebSocket client) and one real submitting `brpc::Channel`: submits three deltas through the input gateway, asserts each synchronous response and each WebSocket broadcast carry the identical, correctly-accumulating `{"sequence_number":N,"total":M}` JSON — specification.md §15 item 6's deliverable, and the closest thing in this repository to a full deployment. Verified with 10+ consecutive clean runs beyond its first pass, following this session's pattern for anything involving subprocesses and networking. |

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

## A dangling-temporary bug this phase's tests caught again

`counter_codec_test.cpp` hit the same class of bug journal's own tests
caught earlier: a `Payload`/view constructed from a `std::string`
temporary (e.g. `payloadOf(R"({"delta": 5})")`) that's still read on a
*later* statement — the temporary is destroyed at the end of the
statement that constructed it, so the view dangles. Fixed by binding
the literal to a named `const std::string` local first in every
affected test case. Worth calling out again here since it's now
recurred three-plus times across this codebase; the fix is always the
same, and always easy to miss in a one-line test setup.

## Seeing it in action

Run a single node directly (it needs `--peer` and `--data_dir`; `--peers`
defaults to itself, forming a trivial one-node group):

```sh
./build/debug/examples/counter/counter_node \
  --peer=127.0.0.1:8100:0 --data_dir=/tmp/counter-node-0
```

Point an input gateway at it, and an output gateway at the same data
directory (colocated — see
[gateway/output/README.md](../../gateway/output/README.md) for why):

```sh
./build/debug/examples/counter/counter_input_gateway \
  --node_peers=127.0.0.1:8100 --listen_port=8200
./build/debug/examples/counter/counter_output_gateway \
  --data_dir=/tmp/counter-node-0 --resume_file=/tmp/counter-node-0/resume \
  --listen_port=8300
```

Submit a delta and watch the total accumulate, either with the load
generator (open-loop: fires at a fixed rate regardless of response
latency, per specification.md §12's load-testing guidance):

```sh
./build/debug/examples/counter/counter_load_generator \
  --input_gateway_addr=127.0.0.1:8200 --count=100 --rate=50
```

or by hand with any WebSocket client pointed at `ws://127.0.0.1:8300/`
to observe broadcasts, and any brpc client (or `end_to_end_test.cpp`'s
`submitDelta` for the exact shape) to submit `{"delta": N}` bodies.

Once a node has proposed a few deltas, its data directory holds a real
journal — inspect it with `tools/dumper`, or certify it with this
example's own replay binary:

```sh
./build/debug/tools/dumper/dumper --data_dir=/tmp/counter-node-0
./build/debug/examples/counter/counter_replay --data_dir=/tmp/counter-node-0
```

See `tools/replay/README.md` and `tools/dumper/README.md` for what
each prints and what their flags do.

`three_node_smoke_test.cpp` is the fuller multi-node demonstration —
three real processes, a real election, real `Propose` RPCs — and is the
best place to read for the exact flags a multi-node local deployment
needs (`--group`, `--peer`, `--peers`, `--election_timeout_ms`).
`end_to_end_test.cpp` is the fuller full-pipeline demonstration — one
node plus both gateways plus a real submitter and a real WebSocket
observer.

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
