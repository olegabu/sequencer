// Tests for the testable core of RunReplayCheck (specification.md
// §11). Deliberately defines its own tiny state machine rather than
// depending on examples/counter — tools/ links against nothing above
// it in the dependency graph (specification.md §9: "nothing depends on
// examples").

#include "replay_check.hpp"

#include <sequencer/temp_dir.hpp>
#include <sequencer/journal/writer.hpp>

#include <cstring>
#include <filesystem>
#include <string>
#include <vector>

#include <gtest/gtest.h>

namespace sequencer::replay::detail {
namespace {

// Running total, exactly like examples/counter's, but with a
// deliberately visible starting offset — used below to force a real,
// detectable divergence between a "recording" instance and a "replay"
// instance.
class SumStateMachine : public sequencer::StateMachine {
 public:
  explicit SumStateMachine(std::int64_t startingTotal = 0) : total_(startingTotal) {}

  void apply(std::uint64_t, Payload input, OutputCollector& outputs) override {
    std::int64_t delta;
    std::memcpy(&delta, input.data(), sizeof(delta));
    total_ += delta;
    outputs.emit(Payload(reinterpret_cast<const std::byte*>(&total_), sizeof(total_)));
    outputs.designateOutput(0);
  }
  void snapshotSave(SnapshotWriter&) override {}
  void snapshotLoad(SnapshotReader&) override {}

 private:
  std::int64_t total_;
};

Payload payloadOf(const std::int64_t& v) {
  return Payload(reinterpret_cast<const std::byte*>(&v), sizeof(v));
}


// Records a journal by directly driving a SumStateMachine + JournalWriter
// — no node, no braft — exactly the shape of records replay must
// reproduce.
std::filesystem::path recordJournal(const std::vector<std::int64_t>& deltas) {
  const std::filesystem::path dir = sequencer::makeTempDir("replay_check_test");
  journal::JournalWriter writer(dir / "journal");
  SumStateMachine recorder;
  for (std::size_t i = 0; i < deltas.size(); ++i) {
    OutputCollector outputs;
    const std::uint64_t seq = static_cast<std::uint64_t>(i + 1);
    recorder.apply(seq, payloadOf(deltas[i]), outputs);
    writer.append(seq, payloadOf(deltas[i]), outputs.outputs());
  }
  writer.flush(false);
  return dir;
}

TEST(ReplayCheck, IdenticalStateMachineReplaysByteIdentical) {
  const std::vector<std::int64_t> deltas = {5, -2, 10, -13, 100};
  const std::filesystem::path dataDir = recordJournal(deltas);

  ReplayConfig config;
  config.dataDir = dataDir;

  SumStateMachine freshInstance;
  const ReplayResult result = runReplayCheck(config, freshInstance);

  EXPECT_TRUE(result.ok) << result.message;
  EXPECT_EQ(result.recordsChecked, deltas.size());
  EXPECT_FALSE(result.firstDivergentSequence.has_value());
  // Auto-created output dir is cleaned up on success.
  EXPECT_FALSE(std::filesystem::exists(result.replayOutputDir));

  std::filesystem::remove_all(dataDir);
}

TEST(ReplayCheck, DivergentStateMachineIsDetectedAndOutputDirPreserved) {
  const std::vector<std::int64_t> deltas = {5, -2, 10};
  const std::filesystem::path dataDir = recordJournal(deltas);

  ReplayConfig config;
  config.dataDir = dataDir;

  // A replay instance starting from a different total than the
  // recording did — every emitted output will differ, forcing a real,
  // detectable divergence at sequence 1.
  SumStateMachine divergentInstance(/*startingTotal=*/1000);
  const ReplayResult result = runReplayCheck(config, divergentInstance);

  EXPECT_FALSE(result.ok);
  ASSERT_TRUE(result.firstDivergentSequence.has_value());
  EXPECT_EQ(*result.firstDivergentSequence, 1u);
  EXPECT_FALSE(result.message.empty());
  // Left in place for post-mortem inspection.
  EXPECT_TRUE(std::filesystem::exists(result.replayOutputDir));

  std::filesystem::remove_all(dataDir);
  std::filesystem::remove_all(result.replayOutputDir);
}

TEST(ReplayCheck, EmptyJournalReplaysTrivially) {
  const std::filesystem::path dataDir = recordJournal({});

  ReplayConfig config;
  config.dataDir = dataDir;

  SumStateMachine freshInstance;
  const ReplayResult result = runReplayCheck(config, freshInstance);

  EXPECT_TRUE(result.ok) << result.message;
  EXPECT_EQ(result.recordsChecked, 0u);

  std::filesystem::remove_all(dataDir);
}

TEST(ReplayCheck, ExplicitOutputDirIsUsedAndNotAutoRemoved) {
  const std::vector<std::int64_t> deltas = {1, 2, 3};
  const std::filesystem::path dataDir = recordJournal(deltas);
  const std::filesystem::path outputDir = sequencer::makeTempDir("replay_check_test");

  ReplayConfig config;
  config.dataDir = dataDir;
  config.replayOutputDir = outputDir;

  SumStateMachine freshInstance;
  const ReplayResult result = runReplayCheck(config, freshInstance);

  EXPECT_TRUE(result.ok) << result.message;
  EXPECT_EQ(result.replayOutputDir, outputDir);
  // An explicitly-provided output dir is never auto-removed, success
  // or not — it's the caller's, not ours to delete.
  EXPECT_TRUE(std::filesystem::exists(outputDir / "journal" / journal::kManifestFileName));

  std::filesystem::remove_all(dataDir);
  std::filesystem::remove_all(outputDir);
}

}  // namespace
}  // namespace sequencer::replay::detail
