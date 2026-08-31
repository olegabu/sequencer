# Implementation instruction — the FIX session gateway (hffix + owned session core)

*For Claude Code, working in the sequencer repository. Authoritative
reference: `specification.md` §8.12 (the design), §8.11 (delivery rule),
§8.10 (the `InputTransport` shape this resolves), §9 (layout and
dependencies). Prerequisite: `02-instruction-designated-outputs.md` is
complete — in particular its step 4 (`TransportShape` and the
`SessionStream` guard in `RunInputGateway`), which this transport relies
on. Place the provided `gateway-fix-README.md` at `gateway/fix/README.md`
unchanged before writing code; it is part of the deliverable.*

## The decision you are implementing, in one paragraph

FIX transport is built on the hffix parser with a session layer owned by
this repository (§8.12), **not** on a FIX engine. This is a deliberate,
documented deviation from the repository's usual "use a purpose-built
dependency" rule, justified by three architecture-specific facts stated
in §8.12 and restated in the README: the journal is already the resend
store; the load harness needs the same session state machine in
initiator role and cannot use a QuickFIX initiator without becoming the
bottleneck; and hffix leaves threading and allocation to us. If you find
yourself reaching for QuickFIX in the gateway path, re-read §8.12 — its
only role is the conformance-test client in step 6.

## Steps

### 1. Dependencies (§9)

- Add hffix. **Check the pinned vcpkg baseline first**: if an `hffix`
  port exists there, use it; otherwise vendor the header-only library
  under `third_party/hffix/` with its license file, and add a CMake
  interface target. Do not guess which case applies — verify and record
  which in the commit message.
- Add `quickfix` as a **test-only** dependency (it is in the baseline;
  depends on `openssl`, already present). It must not be linked by any
  production binary; enforce via the build graph (a test-only CMake
  target), not by convention.
- Boost.Asio is already available through Boost.Beast (§8.7); use it
  for sockets and TLS. No new networking dependency.

### 2. The session core — `gateway/fix/session/` (§8.12 "Scope")

Build one `FixSession` state machine usable in two roles, `Acceptor`
and `Initiator`, selected at construction. Framing and field access via
hffix (`message_reader`/`message_writer` directly on Asio buffers — no
intermediate message objects, no free-store allocation on the message
path). Implement, in this order, each with unit tests against a
scripted peer:

1. Framing: `BeginString`/`BodyLength`/`CheckSum` handling in both
   directions; malformed framing → session-level `Reject` (35=3) or
   disconnect per the FIX 4.4 rules.
2. `Logon` (35=A): heartbeat interval negotiation; credential
   authentication (tags 553/554) through the chassis's existing
   authentication hook — the session core calls the hook, it does not
   embed a credential store; `ResetSeqNumFlag` handling.
3. Sequence numbers: monotonic inbound expectation and outbound counter;
   **persist exactly these two counters per session**, durably, across
   reconnects and process restarts (a small file or the existing
   gateway resume-position store — nothing else is persisted).
4. `Heartbeat` (35=0) emission on interval; `TestRequest` (35=1) issue on
   silence and reply with `TestReqID` echo; peer-silence detection →
   disconnect.
5. `ResendRequest` (35=2), **outbound side**: serve from the journal.
   The output side of the gateway knows, for each execution report it
   sent, the (session, outbound sequence number) → journal (sequence
   number, output index) mapping; keep that mapping in memory per live
   session and reconstruct it on restart by re-reading the journal via
   the relay from the session's last persisted position. Resends
   re-read and re-send those journal outputs with `PossDupFlag` (43=Y)
   and the original `SendingTime` preserved per FIX rules. **There is no
   outbound message store.** Assert this in a test: kill and restart the
   gateway, issue a resend for a pre-restart range, receive correct
   messages.
6. `ResendRequest`, **inbound side**: on a detected gap, request the
   range from the peer; apply gap fill (`SequenceReset` 35=4 with
   `GapFillFlag`) correctly; reject sequence numbers too low.
7. `Logout` (35=5) handshake, both initiated and received; end-of-day
   reset.
8. TLS via Asio SSL, configurable per listener.

Validation stays at the session layer's minimum (§8.12): framing,
checksum, body length, required session fields. Do **not** build
data-dictionary validation; application-field validation is the codec's
or typed layer's job.

Non-goals for this step: FIXT/FIX 5.0 session layer; session schedules
(start/end times). Leave clearly-marked extension points; do not
implement.

### 3. `InputTransport` and `FixInputTransport` — `gateway/fix/input/` (§8.10 (b), §8.11)

- Introduce the `InputTransport` interface mirroring `OutputTransport`'s
  shape — roughly `start(listen, onRequest, onDisconnect)` / `stop()` —
  and give `RunInputGateway` a `transportFactory` overload paralleling
  `RunOutputGateway`'s existing one (§8.5). Refactor the existing brpc
  input path to be the first `InputTransport` implementation so there
  is one chassis, not a special case plus an interface.
- `FixInputTransport` declares `TransportShape::SessionStream`. Per
  §8.11 and the guard from instruction 02 step 4, the chassis discards
  designated outputs for this transport; the transport consumes the
  receipt for admission confirmation and sequence-number bookkeeping
  only.
- Session logon → `SessionId` creation; logout/disconnect →
  `InputCodec::onDisconnect` (a FIX logout is an explicit event — map it
  as such, distinctly from a socket drop).
- Application messages (anything not session-level) are handed to the
  `InputCodec` as the request; the transport never interprets them.

### 4. `FixOutputTransport` — `gateway/fix/output/` (§8.10, §8.12 "Shape")

- `FixOutputTransport : public OutputTransport`, alongside the WebSocket
  and gRPC transports. `toSession(owner, bytes)` and
  `broadcast(topic, bytes)` receive a pre-built FIX message *body* from
  the `OutputCodec`; the transport adds the session-layer fields
  (`MsgSeqNum`, `SendingTime`, `CheckSum`) via the session core and
  sends.
- `broadcast` topics map to FIX `MarketDataRequest` subscriptions: a
  session that has sent a `MarketDataRequest` for a symbol receives
  that topic's broadcasts; no request, no delivery. This resolves
  §8.10's open topic question the standard way.
- Record the (session, outbound seqNum) → journal position mapping
  needed by step 2.5 as messages are sent.

### 5. The session gateway binary (§8.12 "Shape", §8.11)

- A `RunFixSessionGateway(inputCodec, outputCodec)` entry point running
  `FixInputTransport` and `FixOutputTransport` over **one shared session
  core instance**, so a client's order entry and its execution reports —
  aggressive and passive — travel on the same session.
- Enforce §8.11 end to end and test it: submit an order via FIX; the
  synchronous receipt is never turned into a FIX message; the execution
  report arrives exactly once, from the journal, in sequence-number
  order relative to a passive fill on another order of the same
  session. A test with two orders from one session — one aggressive,
  one resting and then hit — must show journal ordering on the wire.
- A separate, output-only market-data gateway configuration:
  `FixOutputTransport` alone, fed by `broadcast`.

### 6. Conformance suite (QuickFIX as client, test-only)

Drive the gateway with a QuickFIX initiator (its own data dictionary,
FIX 4.4) and assert: logon/logout; heartbeat and TestRequest exchange;
a resend request for a range spanning a gateway restart; an inbound gap
filled by the gateway's own ResendRequest; `ResetSeqNumFlag` logon;
session-level rejection of a malformed message. These are the "does it
interoperate with an engine real clients run" tests, and QuickFIX's
quirk-faithfulness is the point.

### 7. Load sender — `bench/load_generator/` FIX transport (§8.12, §9)

- Add a FIX transport to the load generator using the **same session
  core in `Initiator` role**. It sends application messages on the
  harness's open-loop schedule, timestamps at scheduled send time, and
  correlates replies by a custom tag (user-defined range, 5000–9999)
  carried through the state machine's designated output — for the
  counter, the U1/U2 messages already anticipated for
  `examples/counter`.
- Verify the harness's rig-not-the-bottleneck criterion holds for this
  transport (≥2× the highest target rate on loopback, zero drops).
  This is the measurement that justifies §8.12's reason 2; record the
  number in the load generator's README.

### 8. `examples/counter` integration (§10)

- `CounterFixInputCodec` (U1 → 8-byte delta), `CounterFixOutputCodec`
  (record → U2 to the owning session), a `fix_gateway_main.cpp` using
  `RunFixSessionGateway`, and the U1/U2 definitions. Counter's FIX
  reply is U2 delivered from the journal — not the designated output —
  exactly as §8.11 dictates for a session transport; assert it.
- Add the FIX gateway to the example's local deployment configuration
  alongside the existing gateways.

### 9. Acceptance

- All step-2 session-core unit tests and step-6 conformance tests
  green.
- Step 5's ordering test and instruction-02's `SessionStream` guard test
  green.
- No production binary links `quickfix` (build-graph check).
- No allocation on the message path after warm-up (an allocation
  counter or sanitizer-based check in the session-core tests).
- Determinism replay (§11) unaffected; assert.
- `gateway/fix/README.md` present and unmodified from the provided
  text, except for filling in the measured load-sender rate.

## Explicitly not in scope

- FIXT / FIX 5.0 session semantics; session schedules (§8.12
  "Deferred").
- Data-dictionary validation of application messages.
- Any use of QuickFIX outside the test-only conformance target.
- Any change to the journal format.
