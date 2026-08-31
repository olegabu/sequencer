# Testing

This repository's test suite currently runs 97 `ctest` cases. Most are
focused unit tests for one class or one file in isolation — those are
described in each component's own `README.md`, under its own
`## Testing` section, and aren't repeated here.

This document covers the two categories that cut across component
boundaries and don't fit a one-line "unit test" description:

- **[End-to-end tests](#end-to-end-tests)** — tests that wire together
  real components across a real protocol or process boundary (brpc RPC,
  a real subprocess, a real WebSocket, a real cross-thread journal
  mapping) rather than calling one class's methods directly.
- **[Acceptance-checklist tests](#acceptance-checklist-tests)** — the
  tests that specifically prove each of `docs/specification.md` §14's
  five checklist items, several of which are themselves fault-injection
  drills (a killed process, a stalled signing gateway) rather than
  ordinary assertions.

The two categories overlap: every acceptance-checklist test is also an
end-to-end test, since proving a checklist item requires a real,
running system. They're presented separately here because the
checklist has its own normative structure worth tracing item by item.

## End-to-end tests

"Real" here means: a real object graph talking over a real transport
(brpc RPC, `brpc::Stream`, a real WebSocket, or a real OS process
boundary) — not mocks, fakes, or method calls that skip the transport
layer. Two tiers:

### Multi-process: separate OS binaries, real subprocesses

These spawn the actual compiled binaries (`counter_node`,
`counter_input_gateway`, `counter_output_gateway`) via `fork`/`execv`
(see `examples/counter/tests/child_process.hpp`'s `ChildProcess`), not
in-process stand-ins — so they exercise each binary's real
`main()`/gflags/argv path exactly as a deployment would.

| File | Test case(s) | What's real | What it proves |
|---|---|---|---|
| `examples/counter/tests/three_node_smoke_test.cpp` | `ThreeNodeSmoke.ReplicatesIdenticallyAcrossAllThreeJournals` | 3 `counter_node` processes, a real raft group, real `Propose` RPCs, real leader redirects | specification.md §2.1's "replicas lag, never diverge" — all three replicas' journals compared byte-for-byte after several proposals with leader-redirect following. |
| `examples/counter/tests/end_to_end_test.cpp` | `CounterEndToEnd.SubmitThroughInputGatewayIsObservedThroughOutputGateway` | `counter_node` + `counter_input_gateway` + `counter_output_gateway` (3 processes) + a real WebSocket test client + a real brpc submitter | specification.md §15 item 6's deliverable: the full pipeline, client submit → input gateway → node → journal → output gateway → WebSocket broadcast, with byte-identical JSON on both the synchronous response and the broadcast. |
| `examples/counter/tests/kill_leader_drill_test.cpp` | both cases (see [Acceptance-checklist tests](#2-a-kill-leader-under-load-drill) below) | 3 `counter_node` processes, `SIGKILL` fault injection | Also an acceptance-checklist test — described in full there. |

`examples/counter/demo_rest_websocket.sh` is the non-`gtest` counterpart: the same
three processes, driven entirely by tools *outside* this repository
(`curl` for submission, `websocat` for the WebSocket side) rather than
this repo's own C++ test code. See `examples/counter/README.md`'s
"Seeing it in action" for why curl alone can't do the receiving half.

### Single-process: real components, real transport

These run every component in the same test process, but the components
still talk over a real transport — a real `brpc::Channel`/`Controller`,
a real `brpc::Stream`, a real WebSocket handshake, or a real
memory-mapped journal file read by an independently-opened
`journal::JournalReader` — not a direct method call standing in for the
network.

| File | Test case(s) | What's real | What it proves |
|---|---|---|---|
| `node/tests/node_integration_test.cpp` | `NodeIntegrationTest.ProposeReturnsDenseSequenceNumbersAndDesignatedOutputs`, `NodeIntegrationTest.ProposedInputsAreDurablyJournaled` | One real `NodeImpl` (disk-backed raft log/meta/snapshot, a real brpc server), driven over a real `brpc::Channel` | The apply-thread pipeline (mint sequence number → apply → journal → acknowledge) end to end for a single node, before three-node replication is layered on top. |
| `gateway/input/tests/input_gateway_test.cpp` | `InputGatewayTest.SubmitProposesAndReturnsSequenceNumberAndDesignatedOutput`, `InputGatewayTest.MalformedRequestIsRejectedByCodecWithoutProposing`, `InputGatewayTest.SignatureVerifierRejectionPreventsProposing` | A real single-node `NodeImpl` behind a real `InputGatewayImpl`, submitted to over a real brpc channel exactly as an external client would | specification.md §8.5/§8.6's generic chassis loop (accept → codec→toInput → verify → propose → codec→toOutput → respond), including both rejection paths never reaching `Propose`. |
| `gateway/output/tests/output_gateway_test.cpp` | `OutputGateway.DeliversLiveRecordsInOrderToAConnectedSubscriber`, `OutputGateway.ResumesFromDurablePositionAfterRestartWithoutRedelivering` | A real `OutputGatewayImpl` tailing a directly-synthesized journal, delivered to a real client over a real `brpc::Stream` | specification.md §8.3's contract: in-order live delivery, and restart-without-redelivery via the durable resume position. |
| `gateway/output/tests/restart_drill_test.cpp` | `RestartDrill.RestartedGatewayProducesDisseminationIdenticalToAnUninterruptedRun` | Two real `OutputGatewayImpl` instances (one continuous, one stopped/restarted), real `brpc::Stream` clients | Also an acceptance-checklist test — described in full [below](#4-an-output-gateway-restart-drill). |
| `gateway/relay/tests/relay_gateway_test.cpp` | `RelayGateway.SubscribingFromTheBeginningReplaysAlreadyCommittedHistory`, `RelayGateway.DeliversLiveRecordsAppendedAfterSubscribing`, `RelayGateway.EachSubscriberHasAnIndependentCursorFromItsOwnRequestedSequenceNumber`, `RelayGateway.DeliveredRecordsAreByteIdenticalToTheColocatedJournal` | A real `RelayGatewayImpl` tailing a directly-synthesized journal, served over a real brpc client (this repository's own reference `RelaySubscribeClient`) | specification.md §8.2's relay contract: historical replay and live delivery through one mechanism, independent per-subscriber cursors, byte-identical records. |
| `evidence/tests/signing_gateway_test.cpp` | `SigningGateway.SignsACompleteBlockAndServesAVerifiableInclusionProof`, `SigningGateway.NeverSignsAnIncompleteBlock`, `SigningGateway.TwoIndependentInstancesReadingTheSameJournalProduceIdenticalSignedRoots` | A real `SigningGatewayImpl` + `EvidenceServer`, queried over a real brpc client | specification.md §7.1/§7.2/§8.4: blocks are cut only when complete, real Merkle roots and inclusion proofs are signed and served correctly, and two independent instances reading the same journal produce byte-identical signed roots — the "redundancy is free" claim, checked directly. |
| `sdk/cpp/tests/propose_client_test.cpp` | `ProposeClientTest.ProposesThroughANodeDirectlyAndReceivesAReceipt`, `ProposeClientTest.SignedEnvelopeIsAcceptedAndATamperedOneIsRejectedAtTheGateway` | A real `NodeImpl`; the second case also a real `InputGatewayImpl` configured with `sdk`'s own `makeEnvelopeSignatureVerifier` | `ProposeClient`'s transport-agnostic design working against two real transports, and `sdk`'s client-signing scheme actually plugging into `gateway/input`'s `SignatureVerifier` hook — including a hand-tampered envelope rejected before it ever reaches `Propose`. |
| `gateway/output/tests/websocket_output_transport_test.cpp` | `WebSocketOutputTransport.BroadcastDeliversToConnectedClient`, `MultipleMessagesArriveInOrder`, `BroadcastToUnknownTopicIsANoOp`, `TwoClientsOnDifferentTopicsOnlyReceiveTheirOwn` | A real Boost.Beast WebSocket client over a real socket, against a real `WebSocketOutputTransport` server | The WebSocket `OutputTransport` (originally `examples/counter`'s own file, moved to `gateway/output/` once nothing about it turned out to be counter-specific) actually delivers over the wire, and its path-based topic routing correctly isolates different topics from each other — not just that it compiles against Beast's API. |
| `gateway/output/tests/grpc_output_transport_test.cpp` | `GrpcOutputTransport.BroadcastDeliversToConnectedClient`, `MultipleMessagesArriveInOrder`, `BroadcastToUnknownTopicIsANoOp`, `TwoClientsOnDifferentTopicsOnlyReceiveTheirOwn` — the identical four cases as `websocket_output_transport_test.cpp` above | A real synchronous gRPC C++ client (`grpc::CreateChannel` + generated stub — the same one `grpcurl` uses) over a real socket, against a real `GrpcOutputTransport` server | The real-gRPC `OutputTransport` (brpc speaks only unary gRPC, not streaming — see `gateway/output/README.md`) is held to the identical bar as the WebSocket one: real delivery, real topic isolation, over the actual wire protocol. |
| `gateway/relay/tests/relay_grpc_test.cpp` | `RelayGrpc.SubscribingFromTheBeginningReplaysAlreadyCommittedHistory`, `RelayGrpc.EachSubscriberHasAnIndependentCursorFromItsOwnRequestedSequenceNumber` | A real synchronous gRPC C++ client against a real `RelayGrpcServiceImpl`, tailing a directly-synthesized journal | The real-gRPC counterpart to `relay_gateway_test.cpp`'s brpc-based coverage of the same §8.2 contract. Also caught a real shutdown-hang bug — see `gateway/relay/README.md`'s own section on it. |
| `examples/counter/tests/replay_test.cpp` | `CounterReplay.RecordedJournalReplaysByteIdenticalThroughFreshStateMachine` | The real `CounterStateMachine`, replayed through the real `tools/replay` library, exercised via `ctest` exactly as CI runs it | specification.md §11's determinism gate for this example — record now with one state machine instance, replay later through a completely fresh one, byte-identical output. Run on every push by `.github/workflows/ci.yml`. |

## Acceptance-checklist tests

`docs/specification.md` §14 lists five acceptance-checklist items. Each
is verified by a real, executed test in this repository — not just
argued for in prose. This section covers each item in full; the
top-level `README.md` carries a one-line-per-item summary of the same
mapping.

### 1. Replay is byte-identical on a fresh build

> Replay is byte-identical on a fresh build (§11).

Verified by `examples/counter/tests/replay_test.cpp`'s
`CounterReplay.RecordedJournalReplaysByteIdenticalThroughFreshStateMachine`
(see the end-to-end table above), backed by the library-level tests in
`tools/replay/tests/replay_check_test.cpp`
(`ReplayCheck.IdenticalStateMachineReplaysByteIdentical` and three
others covering divergence detection, an empty journal, and explicit
output directories). `.github/workflows/ci.yml` runs the full suite,
including this test, on every push and pull request against `main`.

### 2. A kill-leader-under-load drill

> A kill-leader-under-load drill — with the load generator running,
> killing the leader process — shows no journal gaps and no divergence;
> clients recover by idempotent resubmission; the new leader's journal
> continues densely.

Verified by
`examples/counter/tests/kill_leader_drill_test.cpp`'s
`AcceptanceDrills.KillLeaderUnderLoadShowsNoGapsNoDivergenceAndContinuedCommits`:

- Starts a real 3-node `counter_node` cluster and a client
  (`LeaderTrackingClient`) that proposes continuously, following
  redirects and retrying on failure exactly as an input gateway would
  (specification.md §8.1).
- Runs 20 commits to let the client discover and cache the current
  leader, then sends that leader process `SIGKILL` — an abrupt,
  ungraceful death, not the graceful `SIGTERM` a normal shutdown would
  use.
- Continues proposing across the fault and asserts the client recovers
  and keeps committing (allowing a small, explicit margin for attempts
  that land exactly inside the election window itself).
- Asserts the new leader's journal continues past the pre-kill count
  with no gap, and that every journal entry the two surviving replicas
  share is byte-for-byte identical — and, going one step further than
  the checklist's literal wording, that the **killed** leader's own
  now-static journal is also identical to the survivors' for everything
  it had already committed before it died.

**A deliberate scope note, not a gap:** this drill never asserts an
exact expected running total after the kill. `CounterStateMachine` has
no idempotency-key deduplication — an explicit, documented
simplification appropriate for the smallest useful example (see
`counter_state_machine.hpp`'s class comment) — so a client that
blindly resubmits a delta which had, in fact, already committed before
its acknowledgement was lost would double-count it. That risk is
specific to this toy state machine's lack of deduplication, not to raft
or journal fault tolerance, which is what this drill actually exercises
— so the drill verifies exactly the invariants the checklist asks for
(density, agreement, continued progress) and nothing that would
entangle it with that separate, already-documented limitation.

### 3. Proof verification, journal-alone reconstruction, and the timeout alarm

> A client verifies a proof against its own retained bytes; proof
> reconstruction from the published journal alone succeeds; the
> proof-timeout alarm fires when the signing gateway is deliberately
> stalled.

Three sub-claims, verified by two files:

- **"Verifies a proof against its own retained bytes"** —
  `sdk/cpp/tests/proof_verifier_test.cpp`: `AcceptsAGenuineRecordReconstructedFromInputAndOutput`
  builds a real signed block (the same way `evidence`'s signing gateway
  would) and verifies `sdk::verifyInclusionProof` against input/output
  bytes the caller supplies itself;
  `RejectsAProofAgainstTheWrongLocallyRetainedInput` proves the
  opposite — the wrong retained bytes must never verify;
  `SingleOutputOverloadMatchesTheGeneralOverload` covers the common
  single-designated-output convenience path.
- **"Proof reconstruction from the published journal alone succeeds"**
  — `sdk/cpp/tests/acceptance_drill_test.cpp`'s
  `AcceptanceDrills.ProofReconstructionFromThePublishedJournalAloneSucceeds`:
  a real `SigningGatewayImpl` signs a real block, then a **fresh**
  colocated `journal::JournalReader` — standing in for a client with
  nothing left of its own but access to the published journal — reads
  `rawRecordBytes` directly off disk and that alone is enough to verify
  the proof.
- **"The proof-timeout alarm fires when the signing gateway is
  deliberately stalled"** — the same file's
  `AcceptanceDrills.ProofTimeoutAlarmFiresWhenTheSigningGatewayIsDeliberatelyStalled`:
  a journal deliberately left short of a full block (specification.md
  §7.1 never signs a partial block, so this signing gateway can never
  make progress, by construction — a real, structural stall, not a
  mocked one) paired with a real `sdk::ProofTimeoutAlarm`; the alarm
  correctly reports nothing overdue while still inside its bound, then
  correctly flags the sequence number once the bound elapses with the
  gateway still stalled.

### 4. An output gateway restart drill

> An output gateway restarted from an arbitrary sequence number
> produces dissemination identical to an uninterrupted run.

Verified by `gateway/output/tests/restart_drill_test.cpp`'s
`RestartDrill.RestartedGatewayProducesDisseminationIdenticalToAnUninterruptedRun`
— the literal, comparative claim, not just "no redelivery"
(`output_gateway_test.cpp`'s
`ResumesFromDurablePositionAfterRestartWithoutRedelivering` covers that
narrower property already):

- **Gateway A** runs continuously, subscribed throughout, delivering
  twelve records straight through — the reference "uninterrupted run."
- **Gateway B** is subscribed, delivers the first five records, is
  stopped, restarted at that arbitrary point (sequence 6 — not a block
  or round boundary), resubscribed, and delivers the remaining seven.
- The test asserts gateway B's concatenated pre-restart and
  post-restart segments are byte-for-byte equal to gateway A's single
  continuous delivery.

### 5. A zone-loss drill

> If deployed across three zones, a zone-loss drill shows the system
> continuing to operate at degraded latency; a single-zone-optimized
> deployment, if used, documents its all-or-nothing exposure
> explicitly.

Two parts:

- **The drill** —
  `examples/counter/tests/kill_leader_drill_test.cpp`'s
  `AcceptanceDrills.ZoneLossKillingAFollowerLeavesTheSystemOperatingOnTheRemainingTwoZones`.
  In a 3-node, one-node-per-zone layout, losing any single node's
  process is a zone-loss event; this drill covers the **follower**
  case (the harder leader case, which additionally requires
  re-election, is covered by item 2's drill above). It `SIGKILL`s a
  follower mid-load and asserts the client keeps committing with only
  a small allowance for attempts landing exactly at the kill, and that
  the two remaining nodes' journals stay dense and in agreement.
  Latency measurement itself is out of this repository's scope
  (specification.md §9, §12 — a separate benchmarking repository) —
  what this drill verifies is the functional claim: the system keeps
  operating.
- **The documentation requirement** — the top-level `README.md`'s
  `## Deployment` section states the single-zone-optimized deployment's
  all-or-nothing exposure explicitly, satisfying the checklist's
  documentation half directly (this repository doesn't itself choose a
  deployment topology, so there's no drill to write for it — the
  requirement is that *a deployment which does* make this choice write
  it down, which the README now models).
