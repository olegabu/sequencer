# The Sequencer — Specification

*Audience: engineers (human and Claude Code) implementing the sequencer
and applications on it. This is the single, self-contained specification
that drives development; no other document is required or assumed.*

## Naming convention

**SEQ**, **SM**, and **JRN** are shorthand used only in diagrams and
informal communication, for **the sequencer**, **the state machine**, and
**the journal** respectively. Documentation, READMEs, and code spell the
full words out — the C++ namespace is `sequencer`, not `seq`; types are
named `StateMachine`, not `SM`. This document uses the full words
throughout and the abbreviations only inside diagrams.

---

## 1. What the sequencer is

> **The sequencer is a library that turns a deterministic state machine
> into a fault-tolerant service with a verifiable history.** It accepts
> opaque, client-signed inputs from many concurrent clients, arranges
> them into a single replicated total order, applies each to the hosted
> state machine exactly once, in order, identically on every replica, and
> appends `{sequenceNumber, input, outputs[]}` — each input and its full
> consequence set — as one atomic record in a replayable journal that is
> the system's authoritative output stream. Blocks of journal entries are
> Merkle-rooted and signed, so origin (client signatures), order (dense
> sequence), outcome (deterministic replay), and operator commitment
> (signed roots) are each independently verifiable by any observer.

Short form: **the sequencer makes state machines survivable and
auditable.** It is the verbs — order, persist, prove, disseminate. The
application is the nouns — what the bytes mean, and to whom. The
sequencer never interprets payload bytes; if a job requires interpreting
bytes, it is application code by definition.

The sequencer is never run alone: it exists only as the harness hosting
an application state machine. Every executable is an application; the
sequencer ships as libraries plus a small number of stock tools. The
consensus substrate is **braft** (Baidu's production C++ Raft
implementation, over brpc) — an implementation detail confined to a
private corner of the harness; no braft type appears in any public
interface.

### 1.1 The three logs (mental model)

1. **The raft log** — braft's internal replication artifact. Contains
   *inputs only* (command sourcing). Private; truncated by snapshots.
2. **The journal** — the sequencer's product (§6). Inputs *and* their
   outputs, one atomic record per input, written identically by every
   replica.
3. **Snapshots** — full-state serializations of the state machine,
   enabling raft-log truncation and fast replica catch-up.

Outputs are never replicated — they are *derived*: every replica computes
identical outputs from identical inputs in identical order. Replication
cost is therefore a function of input size alone.

## 2. Guarantees and trust model

### 2.1 Core guarantees

1. **Total order.** Every admitted input occupies exactly one position
   (a dense, gap-free sequence number, starting at 1) in one global
   sequence.
2. **Exactly-once application.** The state machine observes each input
   once, in sequence-number order, on one thread. Retries are absorbed
   by state-machine-level idempotency (client-supplied keys), never by
   the harness.
3. **Replicated durability.** An acknowledged input is committed by Raft
   quorum: it survives any single-node or (in the recommended
   three-availability-zone placement) single-availability-zone failure,
   and every replica will apply it.
4. **Determinism.** Outputs and state are a pure function of the input
   sequence. Two replicas — or one replica and a later replay — produce
   **byte-identical journals**. This is tested, not assumed (§11).
5. **Verifiability.** Anyone holding the published journal can verify
   each input's client signature (origin), recompute block Merkle roots
   against operator signatures (commitment), and re-execute inputs to
   confirm outputs (correctness).

### 2.2 Trust table

| Threat | Defense | Status |
|---|---|---|
| Node or availability-zone crash | Raft quorum, three-zone placement | closed |
| Operator miscalculates (a bug, a wrong result) | deterministic replay, by anyone | closed |
| Operator fabricates an input | client signatures, persisted in the journal | closed |
| Operator rewrites history after the fact | signed Merkle roots; a client receipt contradicting the published log is a self-contained, court-portable fraud proof | closed (detection, not prevention) |
| Operator serves different histories to different clients | two signed roots for one block are mutual proof of fraud | closed (detection) |
| Operator silently drops an input **before acknowledging it** | client-side timeout and idempotent resubmission; statistically visible if systematic | **open residual** |
| Operator refuses to sign a block's root | a bounded-interval alarm in the client library; gaps in the published root sequence are publicly visible | **open residual (accountability, not prevention)** |
| Byzantine replicas | out of scope — Raft assumes crash faults among operator-controlled nodes; a Byzantine own-node is a root compromise that Byzantine-fault-tolerant consensus would not survive either | explicit non-goal |

Positioning: *verifiable if published honestly*, with the first provable
lie terminal — one valid receipt contradicting the published log voids
trust in the entire history, and the deterrent is that mechanism itself.

### 2.3 Non-goals

No multi-tenancy (one platform, many isolated deployments). No
Byzantine fault tolerance. No built-in business schemas. No key-custody
policy. No cross-group atomic transactions (one Raft group is one total
order; multiple groups scale aggregate throughput and are never joined
atomically).

## 3. Topology

```
clients ──REST/FIX/WS/gRPC/brpc──► INPUT GATEWAYS ──Propose(bytes)──► NODES [harness + state machine]
                                                                        │ each node writes
                                                                        ▼
                                            JOURNAL (memory-mapped file pair)
                         colocated mmap read, same machine as the node (§3.3)
                                   ┌────────────────────────────────┼────────────────────────────────┐
                                   ▼                                ▼                                ▼
                        OUTPUT GATEWAYS (§8.3)         RELAY GATEWAY (§3.3, §8.2)         SIGNING GATEWAY (§8.4)
                      brpc Stream / WebSocket /      re-serves Subscribe verbatim,
                        real gRPC (§8.7, §8.9)       over real gRPC streaming (§8.9)       Merkle roots + proofs
                                   ▼                                ▼                                ▼
                             end clients             OTHER GATEWAYS, other machines             verifiers
                        (browsers, any brpc or       (redundancy, scale, any gRPC-
                          real-gRPC client)             capable language — §8.9)            (inclusion proofs)
```

This phase's actual deployment: output gateways, the relay gateway, and
the signing gateway all run **colocated with the node**, on the same
machine, each independently memory-mapping the same journal — three
readers, no coordination between them, exactly the "any number of
independent, concurrent readers" journal was built for (§6.4). The
output gateway and signing gateway disseminate to *their own* audiences
directly from there (three interchangeable transports on the output
side — §8.7, §8.9 — and signed roots/proofs on the signing side, §8.4).
The relay gateway's own audience is different in kind: not end clients,
but **other gateway processes on other machines** — a second output
gateway, a second signing gateway, or any other journal consumer that
would otherwise have to be colocated too. It reaches them over **real
gRPC streaming** specifically (§8.9) because that is the one transport
in this stack any of those other machines can speak regardless of what
language they are written in, without also needing a colocated
memory-map or brpc's own client library. §3.3 below covers the
structural reasoning; §8.9 covers why gRPC is the specific mechanism,
and why it had to be a second, separate transport from brpc's own
Streaming RPC rather than an extension of it.

Nodes never speak client protocols. Gateways are stateless translators
outside the replication group (contracts: §8). Any replica's journal
serves any consumer: **replicas lag, never diverge.**

A node's network surface is deliberately trivial — two operations:

- `Propose(bytes) → {sequenceNumber, designatedOutput?} | redirect(leader) | error`
  — synchronous, returns after commit, application, and journal append.
- `Subscribe(fromSequenceNumber) → stream of raw journal records` —
  remote journal access for non-colocated consumers; colocated consumers
  may instead memory-map the journal file directly. No filtering, no
  interpretation: raw records only.

### 3.1 brpc server topology

Each node runs exactly **one** brpc server, on one port, wearing three
hats: it serves `Propose` and `Subscribe`; it hosts braft's own internal
replication services (braft attaches its replication RPCs to the same
server rather than running its own); and it exposes brpc's built-in
monitoring pages. An input gateway is a brpc server toward clients (it
happens to speak brpc, REST, and gRPC on that one port — see §3.3) and a
brpc *client* (a pooled channel) toward the leader node — so the
reference request path crosses two brpc servers: client → gateway server
→ node server → the state machine.

### 3.2 Gateway parallelism — required even in minimal deployments

Gateways are the scaling and redundancy tier. Run **at least two parallel
instances of each gateway type** behind ordinary load distribution
(a network load balancer, or a client-side list of endpoints); each
instance is a stateless relay — many client sessions in, a few pooled
brpc channels out to the leader's node server. This insulates the node
group — fixed at three or five members, its serial apply thread the
scarce resource — from all client-facing work: session state,
authentication, rate limiting, and client-signature verification (the
single most parallelizable expensive step, at tens of microseconds per
signature) all scale horizontally with gateway count, while a node sees
only a handful of stable pooled channels rather than thousands of client
sockets. Gateway loss is harmless by contract (§8.1): clients reconnect
to any sibling and resubmit unacknowledged inputs idempotently.
**Submitting directly to a node's `Propose` is supported but is not the
scalable path** — local tests and admin tools may do this deliberately,
to exercise the core without gateway overhead — because every direct
client spends node-side cycles on edge work that gateways exist to
absorb.

Because a node's brpc server incidentally speaks REST and gRPC as well
as brpc — a property of the brpc library itself, not something
deliberately built for this purpose — a client could in principle bypass
gateways entirely and submit directly to a node using any of these
protocols. This is not the recommended path for production traffic; it
remains available primarily for local testing, admin tooling, and
diagnostics, and is noted here only so its existence is not mistaken for
an oversight.

### 3.3 A structural asymmetry, and its mitigation

Input gateways are, by construction, always deployed on machines separate
from nodes: they are pure network relays that never touch a node's local
resources. Output gateways are not symmetric in this respect, and the
asymmetry is sharper than it first looks. Reading the journal via direct
memory-mapping is only possible from a process on the *same machine* as
a node, sharing that machine's CPU, memory bandwidth, and network egress
with the node's own consensus and application work. And `Subscribe` —
§3's remote alternative — is served by **the node's own brpc server**,
the same process handling `Propose` and braft's replication traffic:
routing many remote output gateways through it does not remove the
contention, it only moves it from memory bandwidth to sockets and CPU on
that same process. Neither option, used directly by an arbitrary number
of output gateways, actually gives the output side the isolation the
input side has.

**The fix is the same pattern already used on the input side, applied
symmetrically: a dedicated gateway absorbs the fan-out, so that neither
a node's local resources nor its brpc server are touched by however many
downstream consumers eventually exist.**

**Relay gateway.** Colocated with exactly one replica, it reads that
replica's journal via local memory-map and re-serves the identical
`Subscribe(fromSequenceNumber)` RPC over the network — raw records only,
no interpretation, no filtering, byte-identical to the source. It needs
no application knowledge whatsoever (there is no codec to write), so
unlike input and output gateways it ships as a ready-to-run stock binary
from this repository, exactly as `dumper` does (§9, §8.2).

A relay may attach to **any** replica, leader or follower, since journals
never diverge — a follower's journal is a correct, merely possibly
slightly-behind prefix of the same total order. This gives two properties
for free: relays can be spread across the whole cluster instead of
concentrated on the leader (which already carries `Propose` load), and
running one relay per replica yields redundancy at no extra design cost
— a downstream consumer simply connects to whichever relay is reachable.

With relay gateways in place, the recommendation is now unambiguous
rather than a matter of degree: **output gateways, the signing gateway,
and every other journal consumer should `Subscribe` to a relay, never to
a node directly**, in any deployment beyond local testing — exactly
mirroring the existing rule that clients should submit through an input
gateway, never directly to a node's `Propose`, beyond testing. Direct
node access on either side remains supported, for the same reasons
(tests, admin tools, diagnostics), and remains explicitly not the
scalable path.

This substantially supersedes what would otherwise be the more involved
answer: non-voting **learner nodes** — additional replicas outside the
commit quorum, existing solely to host readers on dedicated hardware.
Because a relay already achieves most of the same isolation by reusing
an existing replica's spare capacity rather than provisioning a new one,
learner nodes are worth keeping only as a further-future option, for
deployments wanting isolation stronger than "shares a follower's spare
cycles with the relay" — not built or specified further here.

## 4. The state machine interface (normative)

```cpp
namespace sequencer {

using Payload = std::span<const std::byte>;   // pointer + length, zero-copy

class OutputCollector {
public:
  void emit(Payload output);          // append one output for this input
  void designateOutput(size_t index); // mark which emitted output, if any,
                                      // is relayed synchronously to the
                                      // submitting client (see §5.2)
};

class SnapshotWriter { public: void write(const void*, size_t); };
class SnapshotReader { public: void read(void*, size_t); };

class StateMachine {
public:
  virtual ~StateMachine() = default;

  // Called on the single pinned apply thread, once per committed input,
  // in sequence-number order. `input` and every emitted output are valid
  // only until the next call to apply() (a reused arena; the state
  // machine must not retain pointers across calls).
  virtual void apply(uint64_t sequenceNumber, Payload input,
                     OutputCollector& outputs) = 0;

  virtual void snapshotSave(SnapshotWriter&) = 0;   // full-state serialize
  virtual void snapshotLoad(SnapshotReader&) = 0;   // exact-state restore
};

// The whole node executable:
int RunNode(int argc, char** argv, std::unique_ptr<StateMachine> stateMachine);

}
```

**`designateOutput`, explained.** A state machine may emit any number of
outputs for one input. `designateOutput(index)` marks at most one of them
as the output relayed synchronously back to the client that submitted
the input (§5.2) — distinct from every output's unconditional journaling
and dissemination via output gateways, which happens regardless. A state
machine may designate none. In the counter example (§10), the single
output — the new running total — is also the designated output, since it
is the only output and the submitting client is its only interested
party; a state machine with multiple interested parties per input (for
example, one emitting a result for the submitter and separate results
for other parties) would designate only the submitter's own.

### 4.1 Determinism rules (binding on every state machine)

Inside `apply()` and the snapshot methods, a state machine MUST NOT:

- read a clock or generate randomness — time, if needed, enters as an
  ordinary sequenced input proposed by an external gateway (a periodic
  time-source facility built into the sequencer itself is a plausible
  future extension, not part of this specification — see the closing
  note);
- perform I/O, syscalls, or network calls;
- read environment variables, thread identifiers, or pointer addresses
  into state;
- use floating point in state or outputs — fixed-point integers only
  (this is also what makes integer SIMD safe: integer arithmetic is
  exactly associative, floating-point arithmetic is not);
- iterate an unordered container into outputs or state — use ordered
  keys;
- depend on structure padding or uninitialized bytes;
- allocate unboundedly — prefer preallocated pools and arenas.

Non-deterministic needs (external prices, external fills, wall-clock
time) are **externalized**: a journal-subscribing gateway observes the
world, acts, and re-proposes results as new sequenced inputs. The state
machine only ever sees sequenced facts.

### 4.2 Admission and execution (recommended state machine structure)

Split each state machine into an **admission layer** — request identity,
idempotency-key deduplication (returning the recorded outcome on
duplicates, checked before all other validation), and validation; a pure
function `(command, state) → accept | reject(reason)` that mutates
nothing — and an **execution core** — pure state mutation on already-
admitted commands, with no knowledge of request identity or reject
reasons. The classification test: a *rejection* touches no state and
produces one clean "no"; an *execution result* is a legitimate outcome
the core reports. The counter example (§10) is simple enough not to need
the split, but the pattern is worth knowing before building anything
larger.

## 5. The node harness

### 5.1 Data flow and threading

```
brpc server (baidu_std + gRPC + HTTP/JSON on one port)
  Propose(bytes) → braft task                       [brpc worker threads]
    → raft replicate → quorum commit                 [braft internals]
    → committed-entry ring → pinned APPLY THREAD:    [one core, hyperthread
        sequenceNumber = ++next                       sibling left empty]
        stateMachine->apply(sequenceNumber, input, outputs)
        journal.append({sequenceNumber, input, outputs})
        acknowledge{sequenceNumber, designatedOutput}
```

Normative points:

- **The sequence number is minted by the harness**, never derived from
  raft indices — this keeps the journal format independent of the
  consensus library and raft an implementation detail.
- **Journal append precedes acknowledgement.** A client holding an
  acknowledgement knows the record is durably journaled on the leader
  and quorum-committed.
- The apply thread performs no cryptography and no allocation after
  warm-up (§5.4).
- The acknowledgement is best-effort delivery of an already-durable
  fact: a client that never receives it resubmits with the same
  idempotency key; the state machine's deduplication returns the
  original outcome, so no input is ever recorded twice.
- The journal is asynchronously flushed to disk by default; readers see
  writes immediately via shared memory regardless of flush timing —
  flush policy governs local crash-survival of one replica's file, not
  reader visibility, and not cluster durability, which quorum already
  provides.

### 5.2 The synchronous reply

`Propose` returns after commit, application, and journal append:

```
Receipt { sequenceNumber: u64 }  +  designatedOutput: bytes?
```

- **No input hash is returned.** A client verifies everything against
  its own retained copy of the submitted bytes; a venue-echoed hash is
  redundant for a diligent client and a trap for a careless one (§7.4).
- **No block identifier is returned.** It is derivable from the sequence
  number by the fixed block rule (§7.1).
- The reply carries no signature. Evidence — a block's signed Merkle
  root and this record's inclusion proof — is produced afterward by the
  signing gateway (§7, §8.4) and becomes available within one block.
- Semantics: *committed at this position; will be deterministically
  processed; outcome per the designated output and the output stream.*

### 5.3 Configuration defaults (eight virtual CPUs, three zones)

| Setting | Default | Note |
|---|---|---|
| Raft synchronous flush | off | the async, memory-mapped journal governs local durability separately from cluster quorum durability |
| Parallel append-entries in flight | 8 | pipelines replication over a roughly one-millisecond inter-zone round trip; a value of 1 (stop-and-wait per follower) produces head-of-line tails under jitter |
| Out-of-order append cache | on | mandatory companion of the above: a follower parks out-of-order batches instead of rejecting and forcing retransmission |
| Out-of-order cache size | 16 | roughly twice the parallelism figure; undersizing silently reintroduces retransmission |
| Election timeout | 1000 ms | never lowered across zones — jitter must never look like leader death |
| brpc event-dispatcher threads | 2 | a single dispatcher is the classic ingestion bottleneck; verify the exact flag name against the pinned brpc release |
| brpc worker concurrency | 6 (of 8 vCPUs) | vCPUs minus the apply core's hyperthread pair |
| Server concurrency limit | unlimited | bound in-flight requests at the client or load generator, not the server |

Client channels use a pooled connection type — a single connection
serializes all parsing on one socket at meaningful request rates.

CPU layout on an eight-vCPU (four physical core) node: core 0 for the
operating system and steered interrupts; core 1 for the apply thread,
**hyperthread sibling left empty** (an occupied sibling shares cache with
the apply core and is a stealth source of tail latency); cores 2–3 for
brpc workers and dispatchers.

### 5.4 Concurrency inventory (normative — the complete picture, once)

Every thread and queue inside one node is enumerated here so a state
machine author never needs to reason about, retune, or reimplement any
of it.

| Thread(s) | Idle behavior | Role |
|---|---|---|
| brpc workers (pool) | park | parse and dispatch `Propose`, `Subscribe`, admin requests |
| brpc event dispatchers | block on socket readiness | hand off to workers |
| braft replication (internal pool) | block | append-entries issue and receive, raft log I/O |
| **the apply thread — exactly one, pinned** | **pure busy-spin — never parks** | drains committed entries, calls `apply()`, writes the journal — the one thread every latency figure in this specification is about |

**The apply thread busy-spins unconditionally — a deliberate decision
made once, on every state machine author's behalf.** Any park-and-wake
scheme (a futex, a condition variable, even a hybrid spin-then-park)
places a wake-up on the critical path of the first entry after every
idle gap: single-digit microseconds typically, but scheduler-dependent,
jittery, and landing precisely at burst onset — a structural source of
tail-latency spikes on the one thread this design optimizes hardest. The
cost of unconditional spin is one core at one hundred percent
utilization at all times; that cost is accepted, and machines are sized
accordingly (§5.3's layout already dedicates one core, with an empty
hyperthread sibling, to exactly this). A park-capable mode may exist as
an explicit, clearly-labeled concession for local development, never as
a default and never in any published measurement.

| Queue | Shape | Producer → consumer | Notes |
|---|---|---|---|
| Committed-entry ring | single-producer/single-consumer, lock-free | braft's apply callback → the apply thread | bounded by braft's own flow control; the apply thread drains a full batch per pass, never one entry at a time |
| *(journal handoff)* | none, by design | the apply thread writes directly; readers pull (§6) | there is no queue here — readers can never block or backpressure the apply thread, and this absence is the point |

**The rule this table exists to state once:** the apply thread interacts
with exactly two things — its input ring (spin-drained) and the journal
memory-map (a direct write) — plus signaling the acknowledgement. It
never parks, never blocks, never enqueues into anything with
backpressure, and performs no cryptography: all operator signing happens
downstream of the journal, in the signing gateway, entirely invisible to
this thread (§7).

## 6. The journal

The journal is the sequencer's product: the single, ordered,
authoritative record of every input and its full consequence set,
readable by any number of independent consumers without ever imposing
cost on the writer. Getting its format and protocol right — and
understanding *why* it is shaped this way — matters more than any other
piece of this specification.

### 6.1 Why two files

A node writes two files: a **data file**, holding variable-length
records appended one after another, and an **index file**, holding a
fixed-size array of `{byteOffset, length}` entries, one per record.

The split exists for two reasons. First, **random access.** Records are
variable length, so finding record number K by scanning the data file
from the start is an O(K) operation. A fixed-size index array turns
"find record K" into a single array lookup — `index[K-1]` — an O(1)
operation regardless of how large the journal has grown, which is what
makes `Subscribe(fromSequenceNumber)` and replay tools practical at any
journal size.

Second, and more importantly, **a well-defined publication signal.** A
reader watching the data file's size cannot safely conclude a record is
complete just because the file grew — it might be observing a
partially-written record mid-append. What a reader actually needs is one
small, fixed-size, atomically-updatable value that says, unambiguously,
"records 1 through N are completely and durably written; read any of
them freely." The index file's header carries exactly that value, called
the committed count.

### 6.2 Record and index format

```
JournalRecord {
  sequenceNumber   u64
  inputLength      u32
  input            bytes          // exactly as proposed; a client
                                  //   signature is embedded inside it
  outputCount      u16
  outputs          repeated { length: u32, bytes }
}

IndexHeader { magic: u32, version: u32, closedCleanly: atomic u32,
              committedCount: atomic u64 }
IndexEntry  { byteOffset: u64, entryLength: u32 }   // entry i ↔ sequence number i+1
```

`magic` and `version` exist to make a bad open fail loudly rather than
silently misread garbage. `magic` is a fixed constant at a known offset
— the same idea as a PNG or ELF file's leading signature bytes — checked
before anything else in the file is trusted; a mismatch means this is
not a valid index file (wrong file, truncated file, preallocated but
never-written space), and the reader refuses to proceed rather than
parsing arbitrary bytes as if they were real offsets. `version` is the
on-disk layout's format version, incremented whenever `IndexHeader` or
`IndexEntry`'s fields change; a reader — including the harness itself, on
restart — that sees an unexpected version refuses to open the file,
instead of interpreting old-layout bytes according to a new layout and
producing silently corrupted offsets with no error at all.

Both files are memory-mapped (`MAP_SHARED`), so any process on the same
machine can read them via ordinary memory loads, with no system call per
read and no inter-process communication overhead — which is also
precisely why the colocated-reader resource-contention concern in §3.3
is real: those memory loads compete for the same memory bandwidth as
everything else on the node's machine.

### 6.3 The write protocol — why the order matters

A record is published in exactly three steps, in this order:

1. Write the record's bytes into the data file, at the next free offset.
2. Write the corresponding `IndexEntry` into the index file, at the slot
   equal to the current committed count.
3. **Release-store** the committed count as `count + 1`.

Readers **acquire-load** the committed count and may then freely read any
entry whose index is below the loaded value. This is the *only*
synchronization in the entire journal: there are no locks, no reader
registration, and readers are structurally invisible to the writer.

The order in steps 1–3 is load-bearing, not stylistic. The release-store
in step 3 is paired with every reader's acquire-load; the release-acquire
relationship in the C++ (and Rust, and Java `VarHandle`) memory model
guarantees that any reader observing the new committed count also
observes *every* memory write that happened before it in the writer's
program order — which is exactly the record's data bytes (step 1) and
its index entry (step 2). Reverse the order — publish the index entry
before the data is fully written, say — and a reader could follow a
valid-looking index entry into data that is not yet fully written:
torn, garbage bytes, indistinguishable from a bug. Data, then index, then
the count: in that order, a reader can never observe an incomplete
record, by construction rather than by convention.

This also gives crash recovery an elegant, largely automatic property.
If a node crashes mid-write — after step 1 but before step 3 — the
committed count was never advanced, so that partial record is simply
invisible to every reader and to the writer's own restart logic: nothing
needs to detect or repair it, because nothing ever counted it as
published. On restart, the harness reads the committed count (call it N)
to learn the next sequence number is N+1, and reads `index[N-1]` to learn
the last complete record's offset and length, from which the next free
byte offset in the data file follows directly. The `closedCleanly` flag
is a convenience for tooling — it records whether the previous process
exited cleanly — but is not required for correctness; the count-based
protocol above is sufficient on its own.

### 6.4 Reading

A reader iterates records lazily — `sequenceNumber()`, `input()`,
`outputCount()`, `output(i)`, or the record's raw bytes — with no
copying. Tailing the live journal means spinning briefly on the
committed count, then backing off (a latency-sensitive colocated reader
may configure pure spin instead). Every consumer tracks its own resume
position durably; restarting from any sequence number yields identical
reads, which is what makes gateway restarts and replay both simple and
safe (§8.2, §8.3, §11).

Readers in languages other than C++ must implement the same
acquire-load semantics precisely — a plain load happens to work on x86
but is not portable to ARM. The reference client library (§9) provides
this correctly per language rather than leaving every consumer to
reimplement the protocol.

## 7. Evidence: blocks, roots, and proofs

Every input carries the submitting client's signature over its exact
wire bytes, persisted inside the journaled input. Verified by default at
the input gateway (§8.1); because the signature persists in the journal,
a missed check is detectable and provable forever by anyone who
re-verifies. A journaled entry lacking a valid client signature is
standalone proof of fabrication.

Operator evidence is entirely **Merkle-based**, produced outside the
node by the signing gateway (§8.4) — an ordinary journal reader, no
different in kind from an output gateway. There is no per-input operator
signature and no signing thread anywhere in a node.

### 7.1 Blocks

Every **1024 consecutive sequence numbers** form one block: sequence
numbers 1–1024 are block 1, 1025–2048 are block 2, and so on. This is a
fixed, deterministic rule — changing the block size is itself a
sequenced administrative command, effective at a stated sequence-number
boundary, never a runtime or wall-clock reconfiguration, which is what
lets a failed-over signing gateway (or a second one, running
concurrently) reproduce identical roots without coordination.

At low input rates a block can take a long time to fill, which lengthens
evidence latency proportionally — an accepted tradeoff for now. A
refinement that closes a block after a bounded time interval regardless
of count (via a sequenced time-source input) is a reasonable future
addition and is deliberately parked, not specified further here.

### 7.2 Roots and proofs

Each block's leaves are `hash(rawRecordBytes ‖ sequenceNumber)` over
**complete journal entries — inputs and outputs alike** — so the signed
root attests to *execution*, not merely admission. Binding the sequence
number into the leaf pins each record to its slot. The block's root is
signed as `signature(root ‖ firstSequenceNumber ‖ lastSequenceNumber)`,
with the bounds explicit in the signed message so a receipt is
self-contained: a verifier checks the claimed sequence number falls
within the *signed* bounds, never a value it had to derive itself.

Signed roots are themselves published (and may be journaled), so an
inclusion proof — roughly ten sibling hashes at the default block size —
is **reconstructible from public data forever**: delivering the proof to
a client is transport of a derivable artifact, not the creation of
evidence. Because block-cutting and signing are both deterministic
functions of the journal, any second signing-gateway instance, reading
any replica's journal, produces identical roots and identical
signatures — the evidence role fails over by simply running another
copy, and running several concurrently costs nothing but redundancy.

Cost: one signature per block plus two record-count hashes to build the
tree — on the order of a few hundred nanoseconds per record, amortized,
at the default block size. Evidence is effectively free at any
throughput this system is likely to reach.

### 7.3 What a receipt proves

An unsigned acknowledgement alone is not portable evidence — provability
begins at the signature. A completed proof that contradicts the
published journal is a **self-contained fraud proof**: the operator
signed one history and published another, both artifacts from the
operator's own key, and one proven contradiction voids trust in the
entire history for every participant. A substituted record at a client's
own sequence number fails verification *at the client*, because no
sibling path can fold the client's own leaf up to a root built over a
different leaf without a hash collision — leaving the client in a loud,
unambiguous no-valid-proof state, never holding evidence for an input it
never sent.

### 7.4 Client obligations (part of the trust story, not a convenience)

- Verify a proof **against locally retained submitted bytes, never
  against anything the venue echoes back.**
- Retain submitted bytes (or at least their own hash of them) until the
  proof verifies.
- Treat a proof's non-arrival within a bounded interval as a first-class
  alarm: reconstruct it from the published journal, escalate, and stop
  submitting if the pattern is systematic.
- Reconcile acknowledgements against the published journal routinely —
  an O(1) check per record.
- Resubmit with the same idempotency key if no acknowledgement arrives
  in time; a duplicate returns the original position's receipt, so a
  client can never end up with two receipts for one input.

## 8. Gateways

> Gateways are stateless translators at the edge of the totally ordered
> record. Input gateways terminate client protocols, verify each input's
> client signature, and forward the bytes untouched to `Propose` —
> nothing invented, nothing remembered. The relay gateway carries the
> journal off a node's machine unmodified, so nothing downstream ever
> touches a node directly. Output gateways consume a relay and
> disseminate in order, filtered per audience — nothing computed,
> nothing able to backpressure the write path. The signing gateway makes
> the journal provable — deterministic, redundant for free, and the one
> consumer whose lag is an alarm rather than mere staleness.

### 8.1 Input gateway contract

MUST: terminate the client protocol (sessions, authentication, transport
security, rate limits); translate each request into state-machine input
bytes exactly once and thereafter treat them as immutable — the bytes
proposed are the bytes journaled, and the client's signature covers
them; verify the client signature; forward to the current leader,
follow redirects, and on timeout resubmit blindly (state-machine
deduplication absorbs duplicates); relay the receipt and designated
output in the client's own protocol; propose a disconnect input on
session loss, if the state machine defines one.

MUST NOT: interpret, enrich, reorder, batch, or conditionally drop
inputs; hold state the state machine depends on; read the journal (a
process that both proposes and disseminates is two gateways in one
binary, each obeying its own contract).

### 8.2 Relay gateway contract

MUST: run colocated with exactly one replica, reading that replica's
journal via local memory-map only; re-serve `Subscribe
(fromSequenceNumber)` over the network with byte-identical records; be
attachable to any replica, leader or follower (§3.3); support any number
of concurrent remote subscribers, since fan-out is precisely the load
this gateway exists to absorb off the node.

MUST NOT: interpret, translate, filter, or reorder anything — a relay
carries no `InputCodec` or `OutputCodec` and needs none, which is why it
ships as a ready-to-run stock binary (§9) rather than something an
application links; consume enough of the shared machine's resources to
affect the colocated node's own latency — a relay's job is narrow enough
that this should never occur, and it is worth monitoring as if it could.

### 8.3 Output gateway contract

MUST: consume a **relay gateway's** `Subscribe` stream (never a node's,
beyond local testing — §3.3) in sequence-number order, tracking a
durable resume position; filter and translate per audience; be
restartable from any sequence number with identical output, since
dissemination is a pure function of the journal; preserve per-client
ordering as journal ordering.

MUST NOT: compute business outcomes — if a value is not in an output
record, it does not exist downstream; acknowledge anything upstream
(reads are invisible to the write path; a slow or dead output gateway
can never backpressure the sequencer, whether it reads from a relay or,
in testing, directly from a node); reorder or coalesce in ways that
change client-visible ordering.

### 8.4 Signing gateway contract

MUST: read the journal; cut blocks by the fixed rule (§7.1); build each
block's Merkle tree over complete entries; sign the root; publish signed
roots and serve inclusion proofs; be deterministic end to end; expose
its own lag as a first-class health metric — its lag alone, among all
consumers, is an alarm rather than staleness, since a root not published
within the bounded interval is client-visible misbehavior by contract.

MUST NOT: sign anything not derived from committed journal contents;
fall silently behind. Run at least two instances — determinism means
every instance produces identical output, so redundancy is free.

### 8.5 Plug interfaces (application-facing, symmetric by design)

```cpp
namespace sequencer {

// Input side — the chassis owns sessions, authentication, transport
// security, signature verification, leader tracking, and retries; the
// codec owns meaning:
class InputCodec {
  virtual Result<Bytes> toInput(const ClientRequest&) = 0;    // request → input bytes
  virtual Bytes toOutput(const Receipt&, Payload designatedOutput) = 0; // receipt → response bytes
  virtual std::optional<Bytes> onDisconnect(const SessionInfo&) = 0;
};
int RunInputGateway(int argc, char** argv, std::unique_ptr<InputCodec>);
// Serves baidu_std, REST (JSON), and gRPC on one port — a native brpc
// capability. Pass-through applications (client wire format already
// equals the state machine's input format) need no codec at all.

// Output side — the chassis owns tailing, resume, transport, and
// backpressure isolation; the codec owns interpretation and routing:
class OutputCodec {
  virtual void toOutput(const RecordView&, Fanout&) = 0;  // record → published bytes,
};                                                        // in order, exactly once
int RunOutputGateway(int argc, char** argv, std::unique_ptr<OutputCodec>);
// Fanout: toSession(owner, bytes) | broadcast(topic, bytes).

}
```

`InputCodec` and `OutputCodec` are named and shaped symmetrically on
purpose: both translate between wire bytes and meaning, one at the
entrance to the total order and one at its exit, and both are the
entirety of the per-application surface at the edge. (An earlier draft
named the output-side interface `OutputHandler` with a method called
`onRecord`; `journalReader` was considered and rejected as a name,
because the interface itself does not read the journal — the chassis
does that and calls the codec per record, exactly mirroring how
`InputCodec` never opens a socket itself.)

### 8.6 Why these interfaces live in the library, not the application

It can look, at first, like the codec is entirely the application's
business — and its *content* is: the sequencer's core deals only in
opaque bytes and needs no interface for that, exactly as `apply()` takes
a raw `Payload` with no translation concept anywhere near it. The
gateway's job is different by definition: it translates between two
*different* byte representations — a client's wire format and the state
machine's input format — and something has to know both shapes to do
that. The question is not whether that knowledge belongs to the
application (it obviously does — only the counter example's author knows
its JSON schema); the question is how a chassis loop, written once and
reused by every application, can invoke that knowledge without knowing
what it is. The chassis loop, unchanged regardless of which application
links it, looks like this:

```cpp
// gateway/input/input_gateway.cpp — written ONCE, in the sequencer
// library, and never touched again for any particular application:
void RunInputGateway(..., std::unique_ptr<InputCodec> codec) {
  while (true) {
    auto request = acceptClientRequest();       // generic: brpc/REST/gRPC
    authenticate(request);                       // generic
    auto input = codec->toInput(request);        // ← the ONE line that
                                                  //   needs application
                                                  //   knowledge
    verifyClientSignature(input);                 // generic
    auto receipt = proposeToLeader(input);        // generic
    auto response = codec->toOutput(receipt, receipt.designatedOutput);
    sendClientResponse(response);                 // generic
  }
}
```

For the counter example concretely: `codec->toInput(request)` is where
`{"delta": 5}` becomes eight raw bytes — a line only `CounterInputCodec`
could write, since only it knows counter's JSON shape. Everything else
in the loop above is identical whether the linked application is
counter, a ledger, or an exchange.

For the compiler to accept `codec->toInput(request)` at all,
`codec` needs a *declared type with a declared method* — and that
declaration, not its content, is `InputCodec`. Without it existing
somewhere the chassis can see, the loop above cannot be written, because
there would be nothing to call. If `InputCodec` were instead declared
inside `examples/counter/`, the sequencer library's own `gateway/`
module — which defines `RunInputGateway` — would have to depend on one
of its own users to compile, which is backwards, and a second
application could not reuse the same chassis loop at all; it would need
its own copy, including its own hand-rolled session handling and leader
tracking, for lack of a shared type to write the loop against.

So the split is precise, not approximate: the *interface* — two or three
method signatures per side, carrying no knowledge of counter, JSON, or
any other application — lives in `gateway/`, in this repository, because
generic chassis code must be written against something. Every
*implementation* — `CounterInputCodec`, `CounterOutputCodec`, and every
future application's equivalents — lives in the application repository
that owns the schema it translates. (An interface is one way to express
this seam; a compile-time template parameter would express the same
seam differently, with the chassis loop templated on the codec type
instead of holding a pointer to a base class. Either way, some fixed
contract must be declared in the library for generic code to exist at
all — the content stays the application's regardless of which mechanism
expresses the seam.)

### 8.7 Transport choice differs by gateway direction

An input gateway needs only brpc: its `RunInputGateway` chassis serves
baidu_std, REST (JSON), and gRPC on one port, all natively provided by
the brpc library, and needs no additional dependency regardless of which
of those three protocols a client uses.

An output gateway's transport depends on its audience. brpc's own
full-duplex **Streaming RPC** is a genuine streaming mechanism and a
natural, zero-additional-dependency choice for consumers that are
themselves brpc-aware — an internal analytics pipeline, or the signing
gateway subscribing to a remote node. This is *not* the same thing as
gRPC-protocol streaming, despite brpc's own gRPC compatibility: the
"gRPC on one port" an input gateway serves above is **unary-call-only**.
brpc's Streaming RPC is a `baidu_std`-protocol-specific mechanism, not
consumable by a real gRPC client at all — §8.9 covers this distinction
in full, and the second, genuinely gRPC-wire-compatible transport built
alongside it for consumers that need one. brpc does **not**, however,
implement the WebSocket
protocol (RFC 6455) — it is not among brpc's supported protocols, and
while brpc's protocol set is extensible via a plugin mechanism,
implementing the WebSocket handshake and frame format as a brpc protocol
plugin is real engineering, not a configuration flag. A genuinely
browser- and language-agnostic output gateway — the entire reason to
choose WebSocket over brpc's own streaming, since it lets any `ws://`
client in any language connect with no brpc or protobuf tooling at all —
therefore needs a **separate, dedicated WebSocket library** alongside
brpc. The counter example's output gateway (§10) uses one for exactly
this reason; Boost.Beast (built on Boost.Asio, a natural fit alongside
this project's other permissively-licensed dependencies) is the
suggested choice, with a lighter-weight library such as `uWebSockets` a
reasonable alternative if a future output gateway needs very high
subscriber fan-out.

### 8.8 Relay versus output gateways — a comparison

Both are colocated journal readers serving subscriptions over
`brpc::Stream`, and both are easy to conflate at a glance. They exist
for structurally different reasons and must not be confused:

| | Relay gateway (§8.2) | Output gateway (§8.3) |
|---|---|---|
| Contract | Carries the journal off a node's machine, byte-identical, unmodified | Translates and filters per audience via an application-supplied `OutputCodec` |
| Consumers | Other gateways (output gateways, the signing gateway) that would otherwise read a node's journal directly | End clients — applications, browsers, anything an `OutputCodec` targets |
| Delivery | Any starting sequence number: `Subscribe(fromSequenceNumber)` serves historical replay and live delivery through one mechanism | Live only — a session receives only what's disseminated after it connects |
| Session model | One independent tailing cursor per subscriber, each at its own pace and starting point; no topics, no filtering | Sessions grouped by audience/topic; one dissemination reaches every session in that group |
| Wire content | The complete, raw journal record | Whatever bytes the codec's `toOutput` produces |
| Interprets input | Never — no `InputCodec`/`OutputCodec` involved | Yes — that interpretation is the entire point |
| Ships as | A stock binary; no application repository ever implements one (§9) | A chassis an application links, supplying a codec |

Put differently: a relay is infrastructure — a faithful, dumb copy of
the journal that exists purely to absorb fan-out load off a node
(§3.3). An output gateway is where an application's own meaning enters
the picture. A production deployment's output gateways and signing
gateway instances are meant to consume a relay's `Subscribe` stream
rather than a node's directly (§3.3, §8.3); reading a node's journal
colocated is the allowance for local development and small deployments
(§3), not the target architecture at scale.

Both gateways' `Subscribe` is additionally reachable over real gRPC
streaming, alongside `brpc::Stream`, not instead of it — see §8.9. The
comparison above is unchanged by which wire protocol carries it: a
relay is still infrastructure, an output gateway is still where meaning
enters, regardless of whether the bytes travel over `brpc::Stream` or
real gRPC.

### 8.9 Real gRPC streaming, alongside brpc's own — for reach beyond this stack

§8.7 already distinguishes brpc's gRPC compatibility (unary calls,
native, zero extra dependency) from brpc's own Streaming RPC (a
`baidu_std`-protocol-specific mechanism, not gRPC-wire-compatible
despite the name similarity). That gap matters concretely once the
diagram in §3 is taken seriously: a relay gateway's whole purpose is
letting **other gateway processes on other machines** consume the
journal without ever touching a node (§3.3) — and some of those other
processes will not be written in C++, will not link brpc, and will
have no way to speak `baidu_std` at all. brpc's own upstream confirms
this gap is not a documentation oversight but a genuinely unimplemented
capability: ["BRPC兼容GRPC
stream"](https://github.com/apache/brpc/issues/1589) is an open,
unresolved feature request against brpc itself, not a shipped one.
Real, standard gRPC — with mature client libraries in essentially every
mainstream language, unlike brpc's much narrower client ecosystem — is
the one transport a relay's remote audience can be assumed to reach it
with regardless of what they are written in. This is why the §3
diagram routes the relay's outbound arrow through gRPC specifically,
not through brpc::Stream: reach beyond this stack is the entire reason
that hop exists.

Two components, additive to everything above — nothing existing
changes, and both remain opt-in:

**`GrpcOutputTransport`** (`gateway/output/`) is a second, real-gRPC
implementation of the same `OutputTransport` interface §8.5 already
defines, alongside the chassis's default `BrpcStreamTransport` and the
WebSocket transport §8.7 describes — an application picks one via
`RunOutputGateway`'s transport-factory argument, unchanged from before.
Like the WebSocket transport, its wire message is a generic bytes
envelope (`OutputRecord{bytes payload}`), not application-specific
fields: the chassis never interprets an `OutputCodec`'s bytes, so
routing them through a different transport must not change that. A
separate library target, so an application that wants neither gRPC nor
WebSocket never pulls in either dependency just by linking the chassis.

**A second `RelayService`**, real-gRPC, served by the same
`sequencer_relay` stock binary (§8.2) alongside its existing
`brpc::Stream`-based one — on a **separate port**, gated by a
`--grpc_listen_port` flag that defaults to disabled. Two, not one,
because a real `grpc::Server` and brpc's own server (which is what
serves the existing `RelayService`, per §3.1) are two structurally
separate server stacks that cannot share one port the way brpc's own
baidu_std/REST/gRPC-unary triad does (§3.1, §8.7) — brpc's gRPC
compatibility is a protocol brpc's *own* server speaks, not a bridge to
a second, independent gRPC server implementation. The contract being
served is identical either way: `Subscribe(fromSequenceNumber)`,
byte-identical raw records, any starting sequence number, one
independent cursor per subscriber (§8.2) — the wire protocol is the
only thing that differs between the two `RelayService` instances a
`sequencer_relay` process can serve at once.

**Server reflection is enabled** on both components
(`grpc::reflection::InitProtoReflectionServerBuilderPlugin`), so a
generic gRPC tool such as `grpcurl` needs no `.proto` file of its own
to call either — a deliberate ergonomic choice, and one more concrete
point of contrast with brpc, which implements neither gRPC streaming
nor gRPC's reflection service.

### 8.10 Fully-typed gateways: bypassing the generic chassis, and where FIX would fit

Not sketched or built yet, unlike §8.9 — recorded here as a worked
design, and as the general pattern behind it, for the next time a
protocol doesn't fit the byte-opaque chassis cleanly.

**The pattern already exists, on the input side, today.**
`InputCodec`'s whole point (§8.5, §8.6) is that `RunInputGateway`'s
chassis never interprets a request's bytes — but some protocols carry
distinct, named fields at the transport layer itself, not just in a
body a codec parses, and there is no way to express that through a
codec plugged into a chassis built around `Payload body` in,
`Bytes` out. `examples/counter/grpc_input_gateway_main.cpp` is exactly
this case: a real gRPC `CounterSubmitService.SubmitDelta(int64 delta)
→ {sequence_number, total}` needs `delta` to be an actual protobuf
field grpcurl and any generated client can see and set — not a JSON
blob inside an opaque body — which `RunInputGateway` cannot serve no
matter what `InputCodec` is handed to it. Rather than force-fit this
into the chassis or duplicate leader-following from scratch, it
bypasses `RunInputGateway`/`InputGatewayImpl` entirely and reuses only
`gateway::input::detail::NodeProposer` — the chassis's own
"find the current leader, follow redirects, retry" component,
factored out precisely so a standalone gateway like this one can call
it directly. Everything downstream of `propose()` (raft, journaling,
acknowledgement) is exactly the same either way; only the request's
own shape differs. **Output side never needs this bypass**, and this
is not a coincidence: `OutputTransport` (§8.5, §8.7, §8.9) has been a
real, byte-opaque extension point since before this repository had
more than one transport, so a new one is added *by implementing the
interface*, not by working around the chassis.

**A FIX output transport fits the existing extension point directly.**
`FixOutputTransport : public OutputTransport`, alongside
`WebSocketOutputTransport` and `GrpcOutputTransport` — no new
abstraction needed, same as those two. Its `toSession`/`broadcast`
bytes stay opaque to the chassis exactly as every other transport's do
(§8.6): what a `FixOutputCodec` would produce is a pre-built FIX
message *body* (tag=value content), which the transport wraps with the
session-layer fields a FIX engine owns (`MsgSeqNum`, `SendingTime`,
`CheckSum`) before sending. One open question with no clean precedent
elsewhere in this specification: FIX has no notion of a client
"joining a topic" the way a WebSocket URL path or a gRPC
`SubscribeRequest` field does — a FIX session is just a
(`BeginString`, `SenderCompID`, `TargetCompID`) tuple established at
logon. Two resolutions, neither built here: reuse FIX's own
`MarketDataRequest` (a real, standard subscription message) if
`broadcast`'s topic is naturally an instrument symbol; or keep the
transport itself topic-agnostic — every logged-on session receives
everything, and an application-level codec filters if it needs to —
mirroring how the other two transports stay generic about routing.

**A FIX input transport has the same two choices as the counter gRPC
example above, at library scope instead of one application's scope.**
Either (a) a fully-typed gateway exactly like
`grpc_input_gateway_main.cpp`'s pattern — a small standalone binary
wrapping a FIX engine's `Application` callbacks directly around
`NodeProposer`, no `InputCodec` involved, correct and buildable today
without touching `gateway/input`'s existing chassis at all; or (b) a
new `InputTransport` interface mirroring `OutputTransport`'s shape —
roughly, `start(listenPort, onRequest, onDisconnect)` /`stop()`, with
`RunInputGateway` gaining a `transportFactory` overload exactly
paralleling `RunOutputGateway`'s existing one (§8.5) — so that FIX (and
any future fully-typed input protocol) composes with an arbitrary
`InputCodec` the same way `FixOutputTransport` would compose with an
arbitrary `OutputCodec`. (b) is the architecturally symmetric answer
and the one worth building if more than one non-brpc input transport
ever exists; (a) is strictly less work and ships sooner. Neither is
built or specified further here.

Either way, a FIX engine — not a hand-rolled session layer — should own
`Logon`/`Logout`/`Heartbeat`/`TestRequest`/`ResendRequest` and
sequence-number bookkeeping, the same reasoning behind Boost.Beast for
WebSocket (§8.7) and the standard gRPC library for §8.9: pick a
focused, purpose-built dependency for the protocol's own machinery
rather than reimplement it. QuickFIX is the concrete candidate: the
mature, widely-used open-source C++ FIX engine, and — checked directly
against this repository's own pinned vcpkg baseline, not assumed —
already available there as the `quickfix` port, depending only on
`openssl` (already a dependency of this repository, §9.1) beyond
vcpkg's own tooling packages. `Application::onLogon`/`onLogout` map
naturally onto `SessionId` creation/teardown and `InputCodec`'s own
`onDisconnect` hook (§8.1's "propose a disconnect input on session
loss") arguably better than brpc's implicit connection-close detection
does, since a FIX logout is an explicit, named event rather than a
socket merely going away.

## 9. Repository layout and build tooling

```
sequencer/
├── node/            # the harness: RunNode, the apply thread, sequence-
│   └── raft/        #   number minting, acknowledgement — raft/ is
│                     #   private to node/: nothing else in this tree
│                     #   depends on it, and no braft type is visible
│                     #   outside it
├── journal/         # writer, reader, RecordView — depends on nothing
├── gateway/
│   ├── input/       # RunInputGateway chassis + InputCodec interface —
│                     #   an application links this and supplies a codec
│   ├── output/      # RunOutputGateway chassis + OutputCodec interface —
│                     #   likewise, application-linked
│   └── relay/       # the relay gateway (§8.2): reads a colocated
│                     #   replica's journal, re-serves Subscribe over the
│                     #   network, byte-identical, no codec involved.
│                     #   Needs no application knowledge, so this
│                     #   directory builds a complete, ready-to-run
│                     #   binary — `sequencer_relay` — directly in this
│                     #   repository. No application repository ever
│                     #   implements one; every deployment (this
│                     #   repository's own counter example included)
│                     #   simply runs the prebuilt binary, exactly as it
│                     #   would run `dumper`
├── evidence/         # block-cutting, Merkle tree, the signing gateway
├── sdk/             # the reference client library, per language:
│                     #   propose, verify proofs, reconcile against the
│                     #   journal, raise the proof-timeout alarm — and,
│                     #   because verification requires reading the
│                     #   journal, the per-language journal reader lives
│                     #   here rather than in a separate bindings folder
├── tools/
│   ├── replay/      # a library, invoked the same way RunNode is: it
│                     #   needs a concrete StateMachine to replay against,
│                     #   so it cannot be a standalone generic binary —
│                     #   an application links it and exposes its own
│                     #   replay executable (see §10, §11)
│   └── dumper/      # a genuinely standalone binary: dumps raw journal
│                     #   records (sequence numbers, lengths, a hex or
│                     #   text preview) for human inspection, without
│                     #   interpreting payload meaning — needs no
│                     #   application code, unlike replay
├── examples/
│   └── counter/     # the one example — see §10
├── vcpkg.json       # dependencies: braft, brpc, protobuf, gtest, benchmark,
│                     #   boost-beast (WebSocket, output gateways only — see §8.7)
└── CMakeLists.txt   # CMake with the Ninja generator
```

Two tools that appeared in earlier drafts of this design are deliberately
absent. A `publisher` tool is unnecessary: `Subscribe` is already served
directly by every node (§3), so a separate process republishing the same
stream would be pure redundancy. A generic `rewriter` (for example, one
converting the journal to a columnar analytics format) is not part of
this repository's scope; it is exactly an `OutputCodec` implementation,
and belongs in whichever application repository needs one.

Dependency arrows, enforced by the build graph rather than by review:
`journal` depends on nothing; `node` depends on `journal` (and privately
on braft, via its own internal `raft/` subdirectory); `gateway` and
`evidence` depend on `journal` and on the small set of public types
`node` exposes (`Receipt`, `RecordView`); `sdk` depends on `journal` and
`evidence`'s proof format; `examples` depends on `node`, `gateway`, and
`journal`; nothing depends on `examples`.

Applications live in **separate repositories**, pin the sequencer at a
tagged version, implement the three plug points — `StateMachine`,
`InputCodec`, `OutputCodec` — and ship thin executables:

```cpp
// an application's whole node executable:
int main(int argc, char** argv) {
  return sequencer::RunNode(argc, argv, std::make_unique<MyStateMachine>());
}
```

**Testing and micro-benchmarking:** Google Test for correctness; Google
Benchmark for in-process component measurements (journal append cost,
one state machine's `apply()` cost) where they help catch regressions
during development. Cross-process, cross-machine latency and throughput
measurement — open-loop and closed-loop load generation, tuning sweeps,
published performance tables — is out of scope for this repository and
lives in a separate benchmarking repository, built against this one.

### 9.1 Header-only versus compiled — decided per component, not uniformly

**`journal/` is header-only.** It depends on nothing (§9's layout
already states this), it sits on the single most performance-sensitive
path in the system — every apply-thread write, every reader's
`RecordView` access — and it is exactly the kind of small, hot,
dependency-free code that benefits from being inlined into whatever
calls it: a state machine's `apply()`, a gateway's tailing loop, a
relay's read path. It also means a small third-party tool that only
wants to parse the journal format never has to link braft, brpc, or
protobuf to do so. The verification-only slice of `sdk/` — hashing a
record, walking sibling hashes, checking a signature — is header-only
for the same reason: it needs only a cryptography library, and a client
embedding just proof verification should not have to link the full
propose-and-reconcile machinery to get it.

**`node/`, `gateway/`, and `evidence/` are conventionally compiled
static libraries.** All three wrap substantial external compiled
dependencies — braft, brpc, protobuf, Boost.Beast/Asio, a cryptography
library — and making the *wrapper* header-only buys nothing there, since
linking those dependencies is unavoidable either way; the only effect
would be reparsing their already-expensive headers in every including
translation unit. The inlining argument does not apply to these
components' entry points either: `RunNode` and `RunInputGateway` are
each called once per process lifetime, not from a hot loop, so there is
nothing repeated to inline away.

A related question — whether `StateMachine` should be a template
parameter instead of a virtual interface, letting the compiler inline
`apply()` directly into the harness rather than dispatching through a
vtable — is deliberately declined. §12 already states the actual
latency budget: the commit floor is on the order of half a millisecond
to a full millisecond, and `apply()` itself costs low single-digit
microseconds; a virtual call's overhead is nanoseconds, noise against
both figures. Removing it would optimize something that is not the
bottleneck, at the real cost of losing the clean, type-erased plug-point
pattern the gateway and codec interfaces (§8.5) already rely on for the
same reason.

**Link-time optimization, not header-only, is the lever for the
compiled components.** Enabling LTO for Release builds of `node/`,
`gateway/`, and `evidence/` recovers most of header-only's cross-
boundary inlining where it would genuinely matter, without paying the
header-reparse cost in every translation unit — the right tool given
real compiled dependencies are unavoidable regardless.

## 10. The counter example

The one example in this repository, and the complete demonstration of
every plug point end to end.

**State machine.** Input: an 8-byte signed delta. State: one running
total. Output: the new total after applying the delta — also the
designated output, since it is the only output and the submitting client
is its only interested party.

**Codecs.** `CounterInputCodec` translates a small JSON body (for
example `{"delta": 5}`) into the 8-byte input, and a receipt plus
designated output back into a JSON response. `CounterOutputCodec`
translates each journal record into a JSON message (sequence number and
new total) published over WebSocket.

**Layout:**

```
examples/counter/
├── counter_state_machine.{hpp,cpp}
├── counter_input_codec.{hpp,cpp}
├── counter_output_codec.{hpp,cpp}
├── node_main.cpp             # sequencer::RunNode(...)
├── input_gateway_main.cpp    # sequencer::RunInputGateway(...) — brpc/REST/gRPC
├── output_gateway_main.cpp   # sequencer::RunOutputGateway(...) — WebSocket
│                              #   via Boost.Beast (§8.7); brpc has no native
│                              #   WebSocket support, so this is the one place
│                              #   the example depends on something beyond brpc
├── replay_main.cpp           # links tools/replay against CounterStateMachine
├── load_generator_main.cpp   # a small open-loop-capable client, exercised
│                              #   in tests — a smoke test and a rough
│                              #   throughput/latency sanity check, not a
│                              #   substitute for the benchmarking repository
├── tests/                    # Google Test: state-machine unit tests, a
│                              #   replay test, and a three-local-node
│                              #   end-to-end test driven by the load
│                              #   generator
└── CMakeLists.txt
```

Running the example end to end means: three nodes, at least one
**`sequencer_relay` instance per node** (the prebuilt binary from
`gateway/relay/` — nothing in `examples/counter/` implements this; the
example's deployment configuration only runs it), at least two input
gateway instances, at least two output gateway instances (§3.2) each
consuming a relay rather than a node directly (§3.3), the load generator
submitting deltas through an input gateway, and a WebSocket client (or
the same load generator) observing running totals through an output
gateway — the full topology of §3, relay tier included, at the smallest
useful scale. A local docker-compose setup for this — three node
services, three `sequencer_relay` services, two input-gateway services,
two output-gateway services — belongs alongside the example and, being
pure configuration referencing prebuilt binaries and images, needs no
further specification here.

## 11. Determinism certification

Replaying a recorded input sequence through a fresh build of a state
machine and comparing the resulting journal **byte-for-byte** against
the original is the sequencer's warranty. Because it needs a concrete
state machine, it is a library — `tools/replay` — invoked the same way
`RunNode` is:

```cpp
int RunReplayCheck(int argc, char** argv, std::unique_ptr<StateMachine>);
```

An application links this and exposes its own replay binary (as the
counter example does). Any divergence is a bug — in the state machine
(a §4.1 rule violated), the harness, or the journal protocol itself. Run
it in continuous integration on every example and application; after any
tuning change touching replication pipelining or caching; and treat a
state machine as certified only once replay is byte-identical and §4.1
holds under review.

## 12. Performance model (expectations, not measurements)

This repository does not include load-generation rigs or publish
performance tables — see the separate benchmarking repository for
methodology and results. The design choices in §5 are nonetheless driven
by a specific latency budget, worth stating so the reasoning is legible.

Per input, across three availability zones: consensus commit is bounded
below by one inter-zone network round trip — typically half a
millisecond to a full millisecond, a property of cloud network physics,
not of this software. Applying an input to a reasonably written state
machine costs low single-digit microseconds; a journal append costs well
under a microsecond. The network round trip dominates by a wide margin,
which is exactly why the apply thread is designed to add nothing on top
of it: it never blocks, never allocates after warm-up, never performs
cryptography, and never waits on anything but its own already-committed
input.

Actual throughput and latency depend on hardware, network placement,
tuning, and the hosted state machine's own cost, and are deliberately not
asserted as fixed numbers here. Measuring them honestly — open-loop and
closed-loop, at stated durability settings and placement — is the entire
purpose of the separate benchmarking repository, and every number it
publishes should be reproducible starting from this repository's
`examples/counter` alone.

## 13. Deployment

**Recommended: one node per availability zone, across three zones.**
Survives the loss of any single zone at the roughly one-millisecond
commit floor described in §12. Placing a majority of nodes in one zone
lowers commit latency substantially but means that zone's loss halts the
system entirely — a legitimate choice for a single-zone-optimized
deployment, but one that should be labeled as such rather than presented
as the general recommendation. A five-node, two-zones-plus-one layout
does not improve median latency (one inter-zone round trip is still on
the critical path) but improves tolerance to a second failure and tends
to tighten the tail, since quorum is satisfied by the faster of several
remaining acknowledgements rather than by a single fixed pair. Placement
of the majority, not the total node count, is the lever that trades
latency against resilience.

**Failover is automatic and lossless.** On leader failure the cluster
re-elects without operator intervention; no committed record is lost,
the new leader's journal continues densely, and clients resubmit
unacknowledged in-flight inputs idempotently against the new leader.
Stated honestly: re-election takes on the order of seconds — several
orders of magnitude above the sub-millisecond request latencies this
system targets — yet far below a classical failover to a backup site,
and with no one paged before service resumes. This self-healing property
is a core reason a replicated financial system belongs in the cloud at
all.

**Gateways:** at least two instances of each type in any production
deployment (§3.2); input gateways scale with client count and signature-
verification load, output gateways with subscriber count and fan-out
volume, with the asymmetry and mitigation of §3.3 kept in mind for the
output side. The node group's size never changes for load reasons — only
its placement, and the number of independent groups (see the closing
note on sharding).

## 14. Acceptance checklist

1. Replay is byte-identical on a fresh build (§11).
2. A kill-leader-under-load drill — with the load generator running,
   killing the leader process — shows no journal gaps and no divergence;
   clients recover by idempotent resubmission; the new leader's journal
   continues densely.
3. A client verifies a proof against its own retained bytes; proof
   reconstruction from the published journal alone succeeds; the
   proof-timeout alarm fires when the signing gateway is deliberately
   stalled.
4. An output gateway restarted from an arbitrary sequence number produces
   dissemination identical to an uninterrupted run.
5. If deployed across three zones, a zone-loss drill shows the system
   continuing to operate at degraded latency; a single-zone-optimized
   deployment, if used, documents its all-or-nothing exposure explicitly.

## 15. Implementation order

1. `journal/` — writer, reader, `RecordView`, tailing; unit tests
   including torn-write recovery and the cross-thread acquire/release
   protocol.
2. `node/` (with its private `raft/`) — `RunNode`, the committed-entry
   ring, the pure-busy-spin apply thread, sequence-number minting,
   deferred acknowledgement.
3. `examples/counter`'s state machine and `node_main` — a three-local-
   node smoke test.
4. `tools/replay` as a library, wired into `examples/counter`; the
   determinism gate in continuous integration. `tools/dumper`.
5. `gateway/` chassis — the input side (brpc, REST, gRPC on one port)
   and the output side (journal tailing, `Subscribe` client, transport)
   — plus the `InputCodec`/`OutputCodec` interfaces and the client-
   signature verification hook.
6. `examples/counter`'s codecs, gateway mains, and load generator; an
   end-to-end test submitting through an input gateway and observing
   through an output gateway.
7. `evidence/` — fixed block-cutting, the Merkle builder, the signing
   gateway; `sdk/` — the propose client, proof verification, journal
   reconciliation, and alarms, per language.
8. Kill-leader and zone-loss drills; the full §14 checklist.

Each phase ends green on the checklist items it enables; no phase begins
on a red predecessor.

---

**A short, non-normative note on what is deliberately out of scope
here:** non-voting learner nodes for output-gateway isolation (§3.3); a
bounded-latency, time-tick-based refinement to block-cutting (§7.1);
multi-group deployments, one Raft group per independently-ordered
partition of a larger workload, for scaling aggregate throughput beyond
one group's ceiling; hosting state machines written in languages other
than C++; and alternative consensus substrates to braft. None of these
are required for the sequencer to be complete and useful as specified
above; each is a plausible extension for a later document. The first
real application built on this specification — a financial ledger — is
being developed in its own repository, against the plug points defined
here, rather than inside this one.
