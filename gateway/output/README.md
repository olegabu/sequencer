# gateway/output/

The output side's generic chassis (specification.md
[§8.3](../../docs/specification.md#83-output-gateway-contract),
[§8.5](../../docs/specification.md#85-plug-interfaces-application-facing-symmetric-by-design)):
tail a journal from a durably-tracked resume position, call an
application-supplied `OutputCodec::toOutput(record, fanout)` once per
record, in order, and let the codec route bytes through `Fanout` to
whichever clients care.

## This phase's `journal tailing`: colocated, not yet relay-fed

`RunOutputGateway` reads the journal directly, via a plain
`journal::JournalReader` memory-map on `--data_dir` — specification.md
§3's explicit allowance: "colocated consumers may instead memory-map the
journal file directly." An earlier draft of this component added a
`Subscribe` RPC to `node/` so the output gateway could be a remote
*Subscribe client* instead, matching §8.5's fuller description of this
component ("journal tailing, Subscribe client, transport"); that was
reverted; `gateway/relay/` doesn't exist yet, and until it does, an
output gateway *is* colocated with the node it serves — Subscribe-based
remote tailing is future work for when relay lands, not a gap in this
phase.

## Transport: brpc's own Streaming RPC, delivering live only

`StreamFanout` (`src/stream_fanout.hpp`) is the concrete, built-in
`Fanout` this chassis ships with, built on brpc's own full-duplex
Streaming RPC — specification.md §8.7's zero-additional-dependency
choice for brpc/gRPC-aware consumers. Clients call
`OutputSubscribeService.Subscribe` (attaching a stream first, per
brpc's usual handshake pattern) to join a topic; `Fanout::broadcast`
delivers to every session on that topic, `Fanout::toSession` to one
specific session by id.

**Delivery is live only — there is no historical replay for a client
that subscribes late.** A session only receives records published
*after* it registers; anything broadcast before that is gone (fanout
delivery is deliberately best-effort, not queued). This matches how a
real deployment behaves — clients are normally already connected when
data starts flowing — but it does mean tests must subscribe *before*
appending records they expect to receive; see the test file's own
comments for exactly where this bit an earlier draft.

The **resume position** (`src/resume_position.hpp`) is a different,
gateway-*process*-level concern: it's what makes a restarted gateway
pick up tailing exactly where it left off (specification.md §8.3:
"restartable from any sequence number with identical output"), entirely
independent of which clients happen to be connected at any given moment.

## A real, rare crash this component's tests caught

`gateway/output/tests/output_gateway_test.cpp` intermittently segfaulted
in `OutputGateway.ResumesFromDurablePositionAfterRestartWithoutRedelivering`
— reproducible only when run after many other tests in the same `ctest`
invocation, never in isolation. Root cause, found via `gdb`'s live
thread backtraces: `brpc::StreamClose()` does not guarantee its
stream's `on_closed()` callback has already run by the time it returns
— that callback can fire asynchronously, shortly afterward, from a
different thread. `OutputGatewayImpl::stop()` closed every stream and
then proceeded straight to destroying the `StreamFanout` those
callbacks touch; on rare, unlucky timing, `on_closed()` landed on
already-freed memory. Fixed in `StreamFanout::closeAll()`: it now
tracks every stream it asked to close and blocks (with a bounded
timeout, so a pathological client can never hang shutdown forever)
until each one's `on_closed()` has actually confirmed. Verified against
25 consecutive clean full-suite runs after the fix, versus a
reproducible failure within the first couple of runs before it.

## Testing

```sh
cmake --preset debug
cmake --build --preset debug
ctest --preset debug --output-on-failure
```

`tests/output_gateway_test.cpp` synthesizes a journal directly (no node
needed — `journal::JournalWriter`, matching
`examples/counter/tests/replay_test.cpp`'s pattern) and drives a real
`OutputGatewayImpl` with a real streaming client:

| Case | Proves |
|---|---|
| `DeliversLiveRecordsInOrderToAConnectedSubscriber` | A connected subscriber receives newly-appended records, in order, byte-correct. |
| `ResumesFromDurablePositionAfterRestartWithoutRedelivering` | Stop the gateway mid-stream, append more records, start a fresh instance pointed at the same resume file: a newly-connecting subscriber sees only the new records, never a redelivery of what was already processed before the restart. |

## Seeing it in action

There's no runnable binary here yet, for the same reason as
`gateway/input/`: `RunOutputGateway` needs a codec, and
`examples/counter`'s `CounterOutputCodec` doesn't exist until
specification.md §15 item 6.
