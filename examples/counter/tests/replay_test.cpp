// specification.md §11: "Run it in continuous integration on every
// example and application." This is that gate for the counter example
// — the real CounterStateMachine, replayed through the real
// tools/replay library, exercised via ctest exactly as CI runs it
// (see .github/workflows/ci.yml).
//
// Complementary to three_node_smoke_test.cpp: that test proves
// cross-replica determinism (same build, same run, three processes);
// this one proves cross-time determinism (record once, replay later
// through a completely fresh state machine instance) — together, the
// two claims specification.md §2.1's determinism guarantee makes:
// "two replicas — or one replica and a later replay — produce
// byte-identical journals."

#include "../counter_state_machine.hpp"
#include "replay_check.hpp"

#include <sequencer/temp_dir.hpp>
#include <sequencer/journal/writer.hpp>

#include <cstring>
#include <filesystem>
#include <string>
#include <vector>

#include <gtest/gtest.h>

namespace sequencer::examples::counter {
namespace {


Payload payloadOf(const std::int64_t& v) {
  return Payload(reinterpret_cast<const std::byte*>(&v), sizeof(v));
}

TEST(CounterReplay, RecordedJournalReplaysByteIdenticalThroughFreshStateMachine) {
  const std::filesystem::path dataDir = sequencer::makeTempDir("counter_replay_test");
  const std::vector<std::int64_t> deltas = {5, -2, 10, -13, 100, 0, 42};

  {
    journal::JournalWriter writer(dataDir / "journal");
    CounterStateMachine recorder;
    for (std::size_t i = 0; i < deltas.size(); ++i) {
      OutputCollector outputs;
      const auto seq = static_cast<std::uint64_t>(i + 1);
      recorder.apply(seq, payloadOf(deltas[i]), outputs);
      writer.append(seq, payloadOf(deltas[i]), outputs.outputs());
    }
    writer.flush(false);
  }

  replay::detail::ReplayConfig config;
  config.dataDir = dataDir;

  CounterStateMachine freshInstance;  // a completely independent instance
  const replay::detail::ReplayResult result = replay::detail::runReplayCheck(config, freshInstance);

  EXPECT_TRUE(result.ok) << result.message;
  EXPECT_EQ(result.recordsChecked, deltas.size());
  EXPECT_FALSE(result.firstDivergentSequence.has_value());

  std::filesystem::remove_all(dataDir);
}

}  // namespace
}  // namespace sequencer::examples::counter
