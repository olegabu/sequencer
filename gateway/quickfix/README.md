# gateway/quickfix — the FIX gateway on QuickFIX's session layer

The alternative to [`gateway/fix/`](../fix/README.md), implementing
specification.md §8.13. Both are built and both are supported; the name
is the distinction. `fix` is the one with this repository's own session
core on hffix. `quickfix` is this one.

## Why it exists

`gateway/fix/` owns its session layer, and §8.12 argues for that. The
counter-argument is real: a FIX session layer is mostly edge cases, and
the edge cases are where a hand-rolled engine diverges from the rest of
the world. QuickFIX has had those paths exercised by many
counterparties for two decades; ours has them exercised by our own
tests, which is a weaker claim however many tests there are.

So both exist, and the conformance suite runs against both.

## What QuickFIX does here, and what we do

QuickFIX owns every part of the session protocol: Logon and Logout
handshakes, heartbeat cadence, `TestRequest`/`TestReqID`, inbound
sequence validation, gap detection, generating a `ResendRequest`,
processing an inbound resend, generating `SequenceReset-GapFill`,
applying `PossDupFlag` and `OrigSendingTime` to a replay, and session
scheduling. Every one of the eighteen behaviours
`gateway/fix/tests/fix_session_test.cpp` covers is QuickFIX's
responsibility here.

What we implement is one interface — `FIX::MessageStore` — plus the
wiring that turns `FIX::Application` callbacks into chassis calls.

## The message store is the interesting part

QuickFIX needs a message store per session to answer a
`ResendRequest`. Its stock implementations write every outbound message
to a file, which would put a second copy of every execution report
beside the one the journal already holds, and then require the two to be
reconciled after a crash. §8.12 reason 1 exists to avoid exactly that,
and adopting QuickFIX does not mean giving it up.

`JournalMessageStore` keeps **no message bytes**. Per outbound message it
records one fixed-size row — the journal position the message was
produced from, its MsgType, and the `SendingTime` QuickFIX stamped — and
rebuilds the message on demand by re-reading that journal record and
re-running the output codec over it. The codec is a pure function of the
record, so the reconstruction is deterministic.

It reconstructs a *whole message* rather than a body because QuickFIX's
resend loop does not put `get()`'s output on the wire: it parses each
returned string, reads its MsgType to choose between a gap fill and a
replay, and writes `PossDupFlag` and `OrigSendingTime` into it before
sending (`Session::nextResendRequest`). A complete, parseable message
carrying its **original** sequence number is the contract.

Two consequences worth knowing:

- A message whose journal record has gone, and an administrative message
  that never had one, are **omitted** rather than faked. QuickFIX
  gap-fills whatever the store does not return, which is the correct FIX
  4.4 answer; inventing a message would be worse than admitting the hole.
- `set()` receives a sequence number and bytes and no provenance, so the
  output side calls `noteOrigin()` immediately before asking QuickFIX to
  send. That coupling is the one awkward seam in the design. The
  alternative is storing the bytes, which is the thing being avoided.

## What is smaller here than in gateway/fix

The output transport has no resend source, no catch-up, and no delivery
dedup:

- a `ResendRequest` is answered by QuickFIX out of the store, so the
  transport never sees one;
- when a session logs on, QuickFIX compares sequence numbers and asks
  for what it missed, so there is nothing to replay by hand;
- there is one delivery path, so no high-water mark is needed to stop
  catch-up and a live reader delivering the same record twice.

## The limitation to know before choosing it

QuickFIX 1.15.1's `SocketAcceptor` takes its sessions from
`settings.getSessions()`. There are no dynamic acceptor sessions, so
**every counterparty's CompID must be declared before it connects** —
`--client_comp_ids` on `counter_quickfix_gateway`. `gateway/fix/` adopts
identity from the Logon's `SenderCompID` and needs no such list.

For a venue with known counterparties that is configuration. For one
that accepts whoever arrives, it is a reason to prefer `gateway/fix/`.

## The QUICKFIX_THROW landmine

Read this before touching anything in this directory.

QuickFIX declares its throwing accessors like this:

```cpp
#ifdef __cpp_noexcept_function_type
#define QUICKFIX_THROW(...) noexcept
#else
#define QUICKFIX_THROW(...) throw(__VA_ARGS__)
#endif
```

`__cpp_noexcept_function_type` is defined from C++17 onward. So when
QuickFIX is compiled as C++17 or later — which is the default for any
current GCC, and therefore what vcpkg produces — **every function
documented as throwing becomes `noexcept`**, and the exception it
documents calls `std::terminate` inside the accessor instead of
propagating.

Two consequences, both of which cost real debugging here:

1. **`try`/`catch` around a QuickFIX accessor does nothing.** The
   terminate happens inside the callee, before any handler of ours is
   reachable. `FIX::Message::getField` on an absent field does not throw
   a catchable `FieldNotFound`; it kills the process. Guarding it with a
   try block *looks* correct and is structurally incapable of working.
   Always ask `isSetField` first.
2. **Our own overrides are noexcept boundaries too.** Anything escaping
   `fromApp`, `set`, `get` or any other override with a `QUICKFIX_THROW`
   in its signature terminates the gateway. Those bodies are wrapped so
   nothing escapes.

The same macro is why the conformance suite aborted in CI while passing
locally: `Dictionary::getDay()` threw `ConfigError` to say "StartDay
absent", which `SessionFactory` reads inside a `catch(ConfigError&)` so
the key can be optional — and on a C++17 build that catch is
unreachable. The session configuration here supplies `StartDay`/`EndDay`
for that reason, not because a 24-hour session needs them.

A library being battle-tested is a claim about its protocol logic. It is
not a claim about its build hygiene.

## Tests

`gateway/fix/tests/quickfix_conformance_test.cpp` is a **typed** suite:
every test runs against both gateways, driven by a real QuickFIX
initiator. Twenty-two cases, eleven behaviours each. A behaviour only
one gateway gets right shows up as a single red cell rather than as a
difference nobody looked for.

`tests/journal_message_store_test.cpp` drives the store directly, with a
stub body source standing in for the journal, so a failure there is the
store's and not a gateway's.

## What is not yet measured

Throughput. The hffix arm carries 400k requests/sec with zero drops at a
3.4ms p50 on a five-client fleet; there is no basis in this repository
for predicting where this gateway lands, and QuickFIX is
string-and-allocation based where hffix parses in place. Measure it
before quoting it.
