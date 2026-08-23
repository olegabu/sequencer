#pragma once

// The pinned apply thread's core loop — specification.md §5.1, §5.4:
// "sequenceNumber = ++next; stateMachine->apply(...); journal.append(...);
// acknowledge{...}." This header is braft-agnostic (see
// committed_entry_ring.hpp's comment for why) so it is unit-testable
// with a synthetic producer, with no braft or brpc in the link at all
// — node/tests/apply_loop_test.cpp does exactly that. node/raft/ is
// what wires a real CommittedEntryRing producer and completion callback
// to braft.

#include <atomic>
#include <chrono>
#include <cstdint>
#include <thread>

#include <sequencer/journal/writer.hpp>
#include <sequencer/state_machine.hpp>

#include "committed_entry_ring.hpp"

namespace sequencer::node::detail {

// Invoked once per applied entry, after its sequence number is minted,
// StateMachine::apply() has run, and the record is durably in the
// journal (§5.1: "journal append precedes acknowledgement") — this is
// the harness's entire acknowledgement mechanism. `context` is exactly
// what was passed to CommittedEntryRing::push() for this entry.
using CompletionCallback = void (*)(void* context, std::uint64_t sequenceNumber,
                                     Payload designatedOutput);

class ApplyLoop {
 public:
  ApplyLoop(StateMachine& stateMachine, journal::JournalWriter& journal, CommittedEntryRing& ring,
            CompletionCallback onApplied)
      : stateMachine_(stateMachine), journal_(journal), ring_(ring), onApplied_(onApplied) {}

  ApplyLoop(const ApplyLoop&) = delete;
  ApplyLoop& operator=(const ApplyLoop&) = delete;

  // Processes at most one committed entry. Returns false (without
  // blocking) if the ring was empty. This is the deterministic,
  // single-step primitive tests drive directly; run() below is just
  // this, spun unconditionally.
  bool step() {
    CommittedEntry entry{};
    if (!ring_.tryPop(entry)) {
      return false;
    }

    collector_.reset();
    const std::uint64_t sequenceNumber = journal_.nextSequenceNumber();
    stateMachine_.apply(sequenceNumber, entry.input, collector_);
    journal_.append(sequenceNumber, entry.input, collector_.outputs());

    if (onApplied_ != nullptr) {
      onApplied_(entry.context, sequenceNumber, collector_.designatedOutput());
    }
    return true;
  }

  // The pinned thread's actual body (specification.md §5.4: "the apply
  // thread busy-spins unconditionally — a deliberate decision made once,
  // on every state machine author's behalf"). `stopRequested` is polled
  // with a relaxed load between entries; it is not on any latency-
  // sensitive path, only the shutdown path.
  //
  // `pureSpin` (default true) is the production behavior §5.4 mandates.
  // Passing false trades a small, bounded wakeup latency for not
  // pegging a full core while idle — an explicit, clearly-labeled
  // concession for local development, never a default (§5.4: "A
  // park-capable mode may exist... never as a default and never in any
  // published measurement"). It exists because several node processes
  // sharing one small development machine — as in
  // examples/counter/tests/three_node_smoke_test.cpp — would otherwise
  // each unconditionally peg a core, starving the very raft replication
  // threads leader election depends on.
  void run(const std::atomic<bool>& stopRequested, bool pureSpin = true) {
    while (!stopRequested.load(std::memory_order_relaxed)) {
      if (!step() && !pureSpin) {
        std::this_thread::sleep_for(std::chrono::microseconds(200));
      }
    }
  }

 private:
  StateMachine& stateMachine_;
  journal::JournalWriter& journal_;
  CommittedEntryRing& ring_;
  CompletionCallback onApplied_;
  OutputCollector collector_;
};

}  // namespace sequencer::node::detail
