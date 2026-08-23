# gateway/relay/

specification.md [§8.2](../../docs/specification.md#82-relay-gateway-contract),
[§9](../../docs/specification.md#9-repository-layout-and-build-tooling):
reads a colocated replica's journal via local memory-map only, and
re-serves `Subscribe(fromSequenceNumber)` over the network with
byte-identical records — the piece that lets output gateways and the
signing gateway run on a different machine than the node they consume,
without ever touching that node directly (§3.3's fan-out asymmetry
mitigation). Before this existed, every consumer in this repository
(`gateway/output`, `evidence`'s signing gateway) had to be colocated
with a node itself — the "colocated consumers may instead memory-map
the journal file directly" allowance §3 grants as a stopgap, not a
long-term substitute for a relay.

**This phase does not wire `gateway/output` or `evidence` to consume a
relay instead of tailing colocated** — both remain colocated-only, by
explicit choice, so this is additive: a new, independently-tested
component, not a change to anything else's behavior.

## Why per-subscriber cursors, not a shared broadcast

`gateway/output`'s `Fanout` delivers *live only* — a client that
subscribes late misses everything broadcast before it connected (see
`gateway/output/README.md`). A relay cannot work that way:
`Subscribe(fromSequenceNumber)` must serve *any* already-committed
sequence number, not just "from now on," so historical replay and live
delivery are the same mechanism, not two different code paths.

Concretely, `RelaySession` (`src/relay_session.hpp`) is one per active
`Subscribe` call: it owns a dedicated tailing thread reading from a
shared, colocated `journal::JournalReader`, starting at whatever
`fromSequenceNumber` that particular client asked for. Many sessions
read the same `JournalReader` concurrently — safe and cheap, since
that's exactly the "any number of independent, concurrent readers"
type `journal/` was built for (§6.4). This is what "support any number
of concurrent remote subscribers, since fan-out is precisely the load
this gateway exists to absorb off the node" (§8.2) means in practice:
each subscriber's pace and starting point are entirely its own.

## A third instance of the StreamClose()/on_closed() race — caught before shipping this time

`gateway/output/README.md` documents two real, previously-shipped
segfaults from the same root cause: `brpc::StreamClose()` does not
guarantee its stream's `on_closed()` callback has already run by the
time it returns. `RelaySession`'s destructor and
`RelaySubscribeClient`'s destructor (`include/sequencer/relay/subscribe_client.hpp`,
`src/subscribe_client.cpp`) both follow the same fix applied there:
close the stream, then block — bounded, via a condition variable —
until `on_closed()` has actually confirmed, before anything it touches
can be freed. Given this pattern has now caused real crashes twice
already in this repository (once server-side in `gateway/output`, once
client-side in that component's own tests), it was applied here
proactively, on both the server session and the reference client, from
the start — verified against 25 isolated and 15 full-suite consecutive
clean runs before this component was considered done.

## Testing

```sh
cmake --preset debug
cmake --build --preset debug
ctest --preset debug --output-on-failure
```

`tests/relay_gateway_test.cpp` synthesizes a journal directly (no node
needed, matching `gateway/output/tests/output_gateway_test.cpp`'s
pattern) and drives a real `RelayGatewayImpl` with this repository's
own reference `RelaySubscribeClient`:

| Case | Proves |
|---|---|
| `SubscribingFromTheBeginningReplaysAlreadyCommittedHistory` | A client subscribing after records already exist receives all of them — no "subscribe before appending" dance required, unlike `gateway/output`'s Fanout. |
| `DeliversLiveRecordsAppendedAfterSubscribing` | The same session keeps receiving records appended after it connects — historical and live delivery are one mechanism. |
| `EachSubscriberHasAnIndependentCursorFromItsOwnRequestedSequenceNumber` | Two concurrent subscribers, one from the beginning and one from the middle, each receive exactly what they asked for, independently. |
| `DeliveredRecordsAreByteIdenticalToTheColocatedJournal` | What a subscriber receives is compared byte-for-byte against `journal::JournalReader::record(seq).rawBytes()` read directly — specification.md §8.2's literal "byte-identical records." |

## Seeing it in action

```sh
./build/debug/examples/counter/counter_node \
  --peer=127.0.0.1:8100:0 --peers=127.0.0.1:8100:0 --data_dir=/tmp/counter-node-0
./build/debug/gateway/relay/sequencer_relay \
  --data_dir=/tmp/counter-node-0 --listen_port=8500
```

Any brpc client speaking `RelayService` (`proto/relay.proto`) can
subscribe — this repository's own `sequencer::relay::RelaySubscribeClient`
(`include/sequencer/relay/subscribe_client.hpp`) is the reference
implementation; `tests/relay_gateway_test.cpp`'s `CollectingClient`
shows the exact usage pattern, including decoding the raw bytes it
receives back into a `journal::RecordView`.
