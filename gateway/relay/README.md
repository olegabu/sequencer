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

## A second RelayService, in real gRPC — served on a second port

`proto/relay_grpc.proto` + `src/relay_grpc_service_impl.hpp`: the same
`Subscribe(fromSequenceNumber)` contract, served over the real,
standard C++ gRPC library instead of `brpc::Stream`, alongside (not
instead of) the brpc-based `RelayService` above. Gated by
`--grpc_listen_port` (0, the default, disables it — `sequencer_relay`
serves only the original brpc-based service unless this is set).

**Why a second server on a second port, not the same one:** brpc and a
real `grpc::Server` are two entirely separate server stacks — brpc's
own gRPC compatibility (serving `baidu_std` + REST + gRPC "on one
port," per `docs/specification.md` §8.6) is *unary-call-only*. brpc
does not implement genuine gRPC-protocol *streaming* — confirmed
against brpc's own upstream repository: ["BRPC兼容GRPC
stream"](https://github.com/apache/brpc/issues/1589) is an open,
unresolved feature request, not a shipped capability. So relay's
existing `RelayService` (built on `brpc::Stream`, a `baidu_std`-
protocol-specific mechanism) was never actually consumable by a real
gRPC client — a genuine gap for exactly the use case relay itself
exists for (§3.3: gateways, potentially written in other languages,
consuming a relay instead of touching a node directly). This closes
that gap with the real gRPC C++ library.

**Simpler than `RelaySession`, structurally.** gRPC's synchronous
server-streaming API runs each `Subscribe()` call on its own dedicated
thread for the call's entire lifetime, so `RelayGrpcServiceImpl::Subscribe`
just writes the tailing loop directly — no separate session object, no
async callback machinery, no thread to spawn (gRPC already provides
one per call). `RelayGatewayImpl::waitForReader()` is the one small
addition this needed: direct colocated read access for a caller that
isn't going through `startSession`'s brpc-specific session bookkeeping.

**Server reflection is enabled**, so `grpcurl` needs no `.proto` file
of its own to call this — unlike brpc, which does not implement gRPC's
reflection service either.

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

`tests/relay_grpc_test.cpp` covers the real-gRPC `RelayService` above
with a real synchronous gRPC C++ client, against a directly-synthesized
journal (no node needed, matching `relay_gateway_test.cpp`'s own
pattern):

| Case | Proves |
|---|---|
| `SubscribingFromTheBeginningReplaysAlreadyCommittedHistory` | A client subscribing from sequence 0 receives all already-committed records, in order — the same historical-replay guarantee as the brpc-based service, over real gRPC. |
| `EachSubscriberHasAnIndependentCursorFromItsOwnRequestedSequenceNumber` | Two concurrent gRPC subscribers, one from the beginning and one from the middle, each receive exactly what they asked for, independently. |

**A real shutdown-hang bug this test caught.** Both cases initially
hung — not crashed — for the full length of whatever deadline
`grpc::Server::Shutdown()` was given, every run. Root cause: unlike
`GrpcOutputTransport::stop()` (`gateway/output/`), which wakes every
in-flight `Subscribe()` call *itself* (`SessionRegistry::closeAll()`)
before calling `Shutdown()`, `RelayGrpcServiceImpl::Subscribe`'s tailing
loop had no such mechanism — its only way to learn "please stop" was
`ServerContext::IsCancelled()`, which nothing ever set except
`Shutdown()`'s own deadline-expiry cancellation. Calling `Shutdown()`
with no deadline (the default is infinite) meant it waited forever on
a subscriber sitting idle at the tip of the journal, waiting for a
record that would never come — exactly the scenario a real relay
serves most of the time. Fixed with the same fast-wake pattern
`GrpcOutputTransport` already uses: `RelayGrpcServiceImpl::requestStop()`
sets an `std::atomic<bool>` the tailing loop already re-checks every
poll (5ms), called right before `Shutdown()` in both the test and
`run_relay_gateway.cpp`; `Shutdown()`'s own bounded deadline (5s) is
now just a backstop, not the primary mechanism — the same "belt and
suspenders" relationship `gateway/output/README.md` documents for its
own `closeAll()`/`Shutdown()` pair. Cut this test's own runtime from
~10s to ~0.2s; verified against 20 consecutive clean runs after the
fix.

## Seeing it in action

```sh
./build/debug/examples/counter/counter_node \
  --peer=127.0.0.1:8100:0 --peers=127.0.0.1:8100:0 --data_dir=/tmp/counter-node-0
./build/debug/gateway/relay/sequencer_relay \
  --data_dir=/tmp/counter-node-0 --listen_port=8500 --grpc_listen_port=8501
```

Any brpc client speaking `RelayService` (`proto/relay.proto`) can
subscribe on `--listen_port` — this repository's own
`sequencer::relay::RelaySubscribeClient`
(`include/sequencer/relay/subscribe_client.hpp`) is the reference
implementation; `tests/relay_gateway_test.cpp`'s `CollectingClient`
shows the exact usage pattern, including decoding the raw bytes it
receives back into a `journal::RecordView`. Any real gRPC client can
subscribe on `--grpc_listen_port` instead — reflection is enabled, so
`grpcurl` needs no `.proto` file of its own:

```sh
grpcurl -plaintext -d '{"from_sequence_number": 0}' 127.0.0.1:8501 \
  sequencer.gateway.relay.grpc_proto.RelayService/Subscribe
```

`raw_record` in each printed message is the same byte-identical,
base64-encoded journal record `RelaySubscribeClient` receives —
decode it and feed it to `journal::RecordView` exactly as
`tests/relay_grpc_test.cpp`'s `TestGrpcRelayClient::readOne` does, for
a language that has a gRPC client but no brpc one.
