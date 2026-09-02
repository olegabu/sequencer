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
#include <cstdio>
#include <cstdlib>
#include <span>
#include <string>
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
// `designatedOutputs` is in EMISSION order and may be empty
// (specification.md §4, §5.2). Plural since 2026-08-31; the callee must
// copy what it needs before returning, because these Payloads point
// into the arena the next apply() reuses.
using CompletionCallback = void (*)(void* context, std::uint64_t sequenceNumber,
                                     std::span<const Payload> designatedOutputs);

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

    // Off unless SEQ_APPLY_STALL_US is set, and off means no clock read
    // at all -- this runs per record, and instrumentation that measures
    // itself is a mistake this repo has already made once (see the FIX
    // ring reader's stage timers, which were 47.8% of a gateway's CPU).
    static const char* const kStallEnv = std::getenv("SEQ_APPLY_STALL_US");
    static const std::int64_t kStallUs = kStallEnv != nullptr ? std::atoll(kStallEnv) : 0;
    if (kStallUs <= 0) {
      collector_.reset();
      const std::uint64_t sequenceNumber = journal_.nextSequenceNumber();
      stateMachine_.apply(sequenceNumber, entry.input, collector_);
      journal_.append(sequenceNumber, entry.input, collector_.outputs());
      if (onApplied_ != nullptr) {
        onApplied_(entry.context, sequenceNumber, collector_.designatedOutputs());
      }
      return true;
    }

    // Four phases, reported separately, because knowing WHICH one
    // stalled is the whole point:
    //
    //   gap     -- since the previous record was applied. Large gap with
    //              small phases means nothing was waiting for us: the
    //              stall is upstream, in consensus or replication, not
    //              here.
    //   sm      -- the application's own apply().
    //   journal -- the append, including a segment roll.
    //   notify  -- the completion callback, which runs braft's closure
    //              and so releases the client's response.
    using Clock = std::chrono::steady_clock;
    const auto t0 = Clock::now();
    const std::int64_t gapUs =
        lastStepEnd_.time_since_epoch().count() == 0
            ? 0
            : std::chrono::duration_cast<std::chrono::microseconds>(t0 - lastStepEnd_).count();

    collector_.reset();
    const std::uint64_t sequenceNumber = journal_.nextSequenceNumber();
    stateMachine_.apply(sequenceNumber, entry.input, collector_);
    const auto t1 = Clock::now();
    journal_.append(sequenceNumber, entry.input, collector_.outputs());
    const auto t2 = Clock::now();
    if (onApplied_ != nullptr) {
      onApplied_(entry.context, sequenceNumber, collector_.designatedOutputs());
    }
    const auto t3 = Clock::now();
    lastStepEnd_ = t3;

    const auto us = [](auto a, auto b) {
      return std::chrono::duration_cast<std::chrono::microseconds>(b - a).count();
    };
    const std::int64_t smUs = us(t0, t1), journalUs = us(t1, t2), notifyUs = us(t2, t3);
    const std::int64_t totalUs = smUs + journalUs + notifyUs;
    if (totalUs >= kStallUs || gapUs >= kStallUs) {
      // Wall clock, so a stall can be lined up against anything else
      // sampled on this host.
      const auto wall = std::chrono::duration_cast<std::chrono::microseconds>(
                             std::chrono::system_clock::now().time_since_epoch())
                             .count();
      std::fprintf(stderr,
                   "[apply-stall] t=%lld.%06lld seq=%llu gap=%lldus sm=%lldus journal=%lldus "
                   "notify=%lldus total=%lldus\n",
                   static_cast<long long>(wall / 1000000), static_cast<long long>(wall % 1000000),
                   static_cast<unsigned long long>(sequenceNumber),
                   static_cast<long long>(gapUs), static_cast<long long>(smUs),
                   static_cast<long long>(journalUs), static_cast<long long>(notifyUs),
                   static_cast<long long>(totalUs));
    }
    return true;
  }

  // Set when the journal filled and this loop stopped. A node in this
  // state cannot apply anything further -- specification.md §5.1 puts
  // the journal append before acknowledgement, so there is no correct
  // way to continue without it -- but it can still say so, which is the
  // entire point: previously the exception escaped the apply thread and
  // took the process down through std::terminate, leaving a
  // "terminate called after throwing an instance of 'std::length_error'"
  // and nothing else. Two benchmark fleets died that way before it was
  // diagnosed.
  bool halted() const noexcept { return halted_; }
  const std::string& haltReason() const noexcept { return haltReason_; }

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
  // A full journal ends this loop rather than the process. step() is
  // left throwing so its callers -- including the unit tests that drive
  // it one entry at a time -- still see the error directly; it is only
  // the long-running thread body that has to survive to report it.
  void run(const std::atomic<bool>& stopRequested, bool pureSpin = true) {
    try {
      runOrThrow(stopRequested, pureSpin);
    } catch (const journal::JournalExhausted& e) {
      halted_ = true;
      haltReason_ = e.what();
    }
  }

 private:
  void runOrThrow(const std::atomic<bool>& stopRequested, bool pureSpin) {
    while (!stopRequested.load(std::memory_order_relaxed)) {
      if (!step() && !pureSpin) {
        std::this_thread::sleep_for(std::chrono::microseconds(200));
      }
    }
  }

  StateMachine& stateMachine_;
  journal::JournalWriter& journal_;
  CommittedEntryRing& ring_;
  CompletionCallback onApplied_;
  OutputCollector collector_;
  std::chrono::steady_clock::time_point lastStepEnd_{};
  bool halted_ = false;
  std::string haltReason_;
};

}  // namespace sequencer::node::detail
