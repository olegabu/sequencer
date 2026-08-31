// Unit tests for CounterStateMachine in isolation — no braft, no
// journal, no process. specification.md §10's whole state machine:
// apply() accumulates an 8-byte signed delta into a running total and
// emits (and designates) the new total.

#include "../counter_state_machine.hpp"

#include <cstring>
#include <filesystem>
#include <string>

#include <gtest/gtest.h>

namespace sequencer::examples::counter {
namespace {

Payload payloadOf(const std::int64_t& v) {
  return Payload(reinterpret_cast<const std::byte*>(&v), sizeof(v));
}

std::int64_t designatedTotal(const OutputCollector& outputs) {
  // Counter designates exactly one output; this helper returns it.
  // EXPECT rather than ASSERT because ASSERT_* may only be used in a
  // void-returning function.
  std::span<const Payload> designated = outputs.designatedOutputs();
  EXPECT_EQ(designated.size(), 1u);
  if (designated.empty()) {
    return 0;
  }
  Payload d = designated[0];
  std::int64_t v;
  std::memcpy(&v, d.data(), sizeof(v));
  return v;
}

TEST(CounterStateMachine, SingleDeltaProducesRunningTotalAsDesignatedOutput) {
  CounterStateMachine sm;
  OutputCollector outputs;
  const std::int64_t delta = 5;

  sm.apply(1, payloadOf(delta), outputs);

  EXPECT_EQ(sm.total(), 5);
  ASSERT_EQ(outputs.outputs().size(), 1u);
  EXPECT_EQ(designatedTotal(outputs), 5);
}

TEST(CounterStateMachine, AccumulatesAcrossMultipleApplyCalls) {
  CounterStateMachine sm;
  const std::int64_t deltas[] = {5, -2, 10, -13};
  const std::int64_t expectedTotals[] = {5, 3, 13, 0};

  for (int i = 0; i < 4; ++i) {
    OutputCollector outputs;
    sm.apply(static_cast<std::uint64_t>(i + 1), payloadOf(deltas[i]), outputs);
    EXPECT_EQ(sm.total(), expectedTotals[i]);
    EXPECT_EQ(designatedTotal(outputs), expectedTotals[i]);
  }
}

TEST(CounterStateMachine, RejectsWrongSizedInput) {
  CounterStateMachine sm;
  OutputCollector outputs;
  const std::int32_t wrongSize = 5;
  Payload badInput(reinterpret_cast<const std::byte*>(&wrongSize), sizeof(wrongSize));
  EXPECT_THROW(sm.apply(1, badInput, outputs), std::runtime_error);
}

TEST(CounterStateMachine, SnapshotSaveAndLoadRoundTripsTotal) {
  const std::string tmpl =
      (std::filesystem::temp_directory_path() / "counter_snapshot_test_XXXXXX").string();
  std::string path = tmpl;
  ASSERT_NE(::mkdtemp(path.data()), nullptr);
  const std::filesystem::path file = std::filesystem::path(path) / "state.bin";

  CounterStateMachine original;
  {
    OutputCollector outputs;
    original.apply(1, payloadOf(std::int64_t{42}), outputs);
    original.apply(2, payloadOf(std::int64_t{-10}), outputs);
  }
  ASSERT_EQ(original.total(), 32);

  {
    SnapshotWriter writer(file);
    original.snapshotSave(writer);
  }

  CounterStateMachine restored;
  {
    SnapshotReader reader(file);
    restored.snapshotLoad(reader);
  }
  EXPECT_EQ(restored.total(), 32);

  // Applying continues correctly from the restored state.
  OutputCollector outputs;
  restored.apply(3, payloadOf(std::int64_t{8}), outputs);
  EXPECT_EQ(restored.total(), 40);
  EXPECT_EQ(designatedTotal(outputs), 40);

  std::filesystem::remove_all(path);
}

}  // namespace
}  // namespace sequencer::examples::counter
