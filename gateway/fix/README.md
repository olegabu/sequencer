# FIX gateway

*Location: `gateway/fix/`. Normative reference: `specification.md`
§8.10–§8.12. This README exists to answer one question before anyone
asks it: why does this directory own a FIX session layer instead of
using QuickFIX?*

## What is here

```
gateway/fix/
├── session/   # the session core: FIX session state machine on hffix
│              #   (framing) + Boost.Asio (sockets, TLS). Two roles from
│              #   one implementation — acceptor (this gateway) and
│              #   initiator (bench/load_generator's FIX sender)
├── input/     # FixInputTransport — implements InputTransport; composes
│              #   with any application's InputCodec
└── output/    # FixOutputTransport — implements OutputTransport; composes
               #   with any application's OutputCodec
```

An order-entry deployment runs `input/` and `output/` as one **session
gateway** binary sharing a session-core instance, because FIX delivers
every execution report for a session's orders — aggressive and passive
— on the session that owns them. Market data is a separate, output-only
session subscribed via FIX's own `MarketDataRequest`.

## Why not a FIX engine

This repository's rule everywhere else is: use a focused, purpose-built
dependency for a protocol's own machinery; do not reimplement it. That
is why WebSocket is Boost.Beast and gRPC is the standard library. By that
rule, FIX should be QuickFIX — mature, widely used, in our vcpkg
baseline. **We deliberately do not follow the rule here**, and the
reasons are specific to this architecture, not a general preference for
writing our own:

**1. We already have a resend store, and it is better than an engine's.**
The heart of a FIX engine is its message store — the persisted log of
every message sent and received, from which `ResendRequest`s are
answered. In this system that log already exists: every execution
report this gateway sends is an output in the sequencer's journal,
routed to the session that owns it, and every inbound message is a
journaled input. Resends are served by re-reading the journal via the
relay gateway. An engine's store would be a second copy of the same
history, with its own persistence, its own failure modes, and a
reconciliation problem against the journal that does not exist if the
journal is the only store. The only session state we persist is two
sequence-number counters per session.

**2. The load-test harness needs a fast session layer regardless.**
Initiator and acceptor are the same state machine in two roles: the
same heartbeats, sequence rules, and resend handling. A QuickFIX
initiator cannot serve as our load sender — its allocation-heavy,
one-thread-per-session design tops out around tens of thousands of
messages per second and adds jitter of its own, so the rig would be the
bottleneck before the gateway is, failing the harness's own
rig-not-the-bottleneck rule. We would have to build a fast, minimal
session layer for the rig anyway. Building it once, usable in both
roles, makes this gateway's session layer close to free and tests it
from both sides.

> **Measured.** The rig criterion is ≥2× the highest target rate with
> zero drops. A single sender reaches **~77,000/s** on loopback with
> zero drops (release build, 2026-08-31; four isolated runs, see
> `bench/load_generator/README.md`). That does not clear 200k on its
> own — but the rig is five client boxes, each with a load generator
> beside its own gateway, so it offers roughly **385k/s**, which clears
> 2× a 100k sweep comfortably. The operational rule that follows: a FIX
> sweep must be run multi-client, exactly as the sequencer sweeps are.
>
> What this does *not* establish is the comparison against QuickFIX.
> ~77k/s per sender is the same order the spec attributes to a QuickFIX
> initiator, so reason 2's claim of outrunning it is **unproven**, even
> though the rig as deployed is fast enough. Reasons 1 and 3 are
> untouched. The sender is deliberately naive — one write per message —
> and coalescing it is the obvious next step, untried.

**3. We keep our own threading and allocation model.** An engine brings
its thread and socket architecture with it, and QuickFIX's is a poor fit
for a process whose siblings follow the sequencer's no-allocation,
controlled-thread discipline. hffix reads and writes fields directly on
an I/O buffer, never allocates, and leaves sockets and threads to us —
so sessions run on the Boost.Asio stack we already depend on, with TLS
from the same place, at ~1–3µs per message parse.

We did weigh the alternatives. QuickFIX: mature, slow, imposes its
threading. Fix8: roughly 3× faster than QuickFIX, but LGPL, with a
community edition whose own maintainers describe its upkeep as
uncertain. Commercial engines: not relevant to an open stack.

## What we own, and what we do not

**Owned:** `Logon` with credential authentication and heartbeat
negotiation; `Heartbeat`/`TestRequest`; monotonic sequence numbers with
per-session persistence of the two counters; `ResendRequest` in both
directions (outbound served from the journal, inbound gaps requested
from the peer); gap fill and `SequenceReset`; session-level `Reject`;
the `Logout` handshake; end-of-day reset; TLS.

**Deliberately minimal:** validation at the session layer is framing,
checksum, `BodyLength`, and required session fields only.
Application-message validation belongs to the codec or typed layer, not
to a data-dictionary engine.

**Deferred:** FIXT / FIX 5.0 session semantics; session-schedule
management. Initial target is FIX 4.4.

## QuickFIX is still here — as a test client

The conformance suite drives this gateway with a real QuickFIX
initiator, so that resend, gap fill, logon edge cases, and sequence
resets are exercised by an engine that other people's clients actually
run. That is the right job for a slow, mature, quirk-faithful
implementation, and the only job it has in this repository.

## Delivery semantics (specification §8.11)

This gateway never returns execution reports as the synchronous reply to
an order. All outputs for a session come from the journal, in
sequence-number order, delivered by the output side. The synchronous
receipt from `Propose` is consumed internally — admission confirmation
and the sequence number for gap and timeout handling — and discarded.
Each output reaches a session exactly once, by that path, never both.

## Recovery: three mechanisms, and which one to reach for

These get conflated, and conflating them produces bugs that look like
protocol failures. They are distinct, and only two of them live here.

**`ResendRequest` — session-scoped recovery.** A client asks for
outbound `MsgSeqNum` 7 through 12. Content-blind and protocol-level, so
it applies to any FIX session, order entry and market data alike. Served
from the journal: the gateway knows which record produced each message
it sent, re-reads that record, and re-runs the codec over it. The same
record through the same codec is the same bytes, which is why no
outbound message store is needed. Resent messages carry `PossDupFlag`
and their original `SendingTime`, as FIX 4.4 requires.

**Catch-up — what the gateway could not send.** An output addressed to a
session that is disconnected is never sent, so the outbound sequence
number never advances for it. The returning client therefore has *no gap
to detect*, and `ResendRequest` — which only replays numbers the gateway
actually sent — correctly has nothing to offer. On Logon the gateway
re-reads the journal from the session's last persisted position and
sends what it missed as **new** messages.

These messages are deliberately **not** flagged `PossDup`: that flag
means "a retransmission of a message previously sent", and the gateway
never sent these. Flagging them would invite a client to discard a real
fill as a duplicate.

Catch-up is bounded (10000 records). A session away for a long time
would otherwise stall its own Logon replaying the whole journal onto it;
past the bound the client resumes from a later point, the same guarantee
FIX gives after an operator-forced reset.

**Journal replay from an arbitrary point — not this gateway's job.**
"Stream me everything from sequence N" is the relay gateway's contract
(§8.2): `Subscribe(fromSequenceNumber)`, byte-identical records, any
number of concurrent subscribers, no codec, shipped as a stock binary.
Point clients there. Adding a user-defined FIX tag for it here would
duplicate a component that already does it — and does it better, since
the relay interprets nothing and so cannot alter what it replays. It
would also need a non-standard tag no off-the-shelf FIX client could
use, and a day-long replay would compete with live delivery on the same
socket.

The dividing line: **`ResendRequest` and catch-up are session-scoped and
belong here; journal-scoped replay belongs to the relay.**

## Two deployment shapes

**Order entry** runs `input/` and `output/` in one process over one
session-core instance, because FIX delivers every execution report for a
session's orders — the aggressive fill on the order just sent, and the
passive fill on one resting since this morning — on the order-entry
session that owns them. Two independent gateways could not: the output
side would have no way to reach the socket the order arrived on.

**Market data** is output-only, fed by `Fanout::broadcast`, with no
`InputCodec` and no raft endpoint — so it can be deployed where an
order-entry gateway should not be, and a compromise of it can submit
nothing. Subscription is FIX's own `MarketDataRequest`.

Both shapes need the journal for recovery. A market-data session is a
FIX session: it drops, reconnects, and recovers by exactly the two
mechanisms above. It is not "tail the journal only".

**Not yet implemented:** `MarketDataRequest`'s `SubscriptionRequestType`
(263) is read as a subscription regardless of its value, so a client
sending `263=0` (snapshot only) is registered for updates and gets no
snapshot. A snapshot is an application-level query the codec or state
machine must answer; the subscription half is what is built.

## Throughput, and the bug that hid it

Measured on one machine, one node, one session, release build
(2026-09-01):

| offered | achieved | p50 | p99 | dropped |
|---|---|---|---|---|
| 2,000/s | 1,982 | 4.8 ms | 16 ms | 0 |
| 5,000/s | 4,955 | 4.9 ms | 21 ms | 0 |
| 10,000/s | 9,893 | 6.4 ms | 19 ms | 0 |
| **20,000/s** | **19,831** | **9.8 ms** | 37 ms | **0** |
| 40,000/s | 37,877 | 82 ms | 135 ms | 13,567 |

So it carries 20k/s cleanly and knees between 20k and 40k, comparable to
the brpc path on the same box.

**An earlier version of this section reported a ceiling of ~1,000/s.
That was wrong by a factor of twenty, and how it went wrong is the part
worth keeping.**

The load generator did not send `ResetSeqNumFlag` on Logon. Each run is
a fresh process with fresh in-memory sequence counters, while the
gateway persists this CompID's numbers across runs — so every run after
the *first* against a given gateway logged on at MsgSeqNum 1 against a
gateway expecting thousands. That is MsgSeqNum-too-low, which FIX treats
as unrecoverable, so the gateway logged the client out and dropped it.
The client kept sending into a dead session, replies never came, and the
harness dutifully reported `dropped-by-rig`.

Every sweep row after the first was therefore measuring a refused
session. The evidence looked exactly like a server that collapses under
load, and it survived a `perf` profile, a syscall trace, stage timers on
both gateway threads, and three commits. What exposed it was running the
*same* configuration three times in a row instead of sweeping upward:
run 1 passed, runs 2 and 3 failed identically. Distinct CompIDs per run
made it disappear.

Two things follow, both now in the code:

- `FixRequester` logs on with `ResetSeqNumFlag`, which is precisely the
  mechanism for "I have no history, start us both at 1".
- `start()` waits a moment and re-checks, because a gateway rejecting a
  logon on sequence grounds sends a valid Logon echo *first* and
  disconnects immediately after — so `isLoggedOn()` is briefly true for
  a session that is already dead.

Stage timers (`FIX_STAGE_TIMERS=1`) remain available in both of the
gateway's loops. Under the corrected setup they still show plenty of
headroom, which is consistent with the knee being above 20k.
