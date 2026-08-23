#pragma once

// The counter example — specification.md §10: the one example in this
// repository, and the complete demonstration of every plug point end
// to end. This file is the state machine plug point.

#include <cstdint>

#include <sequencer/state_machine.hpp>

namespace sequencer::examples::counter {

// Input: an 8-byte signed delta. State: one running total. Output: the
// new total after applying the delta — also the designated output,
// since it is the only output and the submitting client is its only
// interested party (§10).
//
// Simple enough not to need the admission/execution split of §4.2: a
// real state machine with reject reasons and idempotency-key
// deduplication would separate those concerns from this pure state
// mutation; a malformed input here (wrong size) is treated as a
// harness-level contract violation, not a rejectable business outcome,
// and apply() throws rather than silently ignoring it — by design, a
// well-formed CounterInputCodec (§8.5, a later phase) guarantees this
// never happens in the normal request path.
class CounterStateMachine : public sequencer::StateMachine {
 public:
  void apply(std::uint64_t sequenceNumber, Payload input, OutputCollector& outputs) override;
  void snapshotSave(SnapshotWriter& writer) override;
  void snapshotLoad(SnapshotReader& reader) override;

  // Not part of the StateMachine interface — a convenience for tests
  // that want to check state without decoding a journal record.
  std::int64_t total() const noexcept { return total_; }

 private:
  std::int64_t total_ = 0;
};

}  // namespace sequencer::examples::counter
