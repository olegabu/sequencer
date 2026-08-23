#pragma once

// The testable core of RunReplayCheck (replay.hpp), separated from
// argv/gflags parsing exactly as node/src/node_impl.hpp separates
// NodeImpl from RunNode — so a test can drive it directly with an
// explicit config, no process, no CLI.

#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>

#include <sequencer/state_machine.hpp>

namespace sequencer::replay::detail {

struct ReplayConfig {
  // The recorded journal to replay: dataDir/journal.data, journal.index.
  std::filesystem::path dataDir;

  // Where the replayed journal is written. If empty, a temp directory
  // is created — and removed on success; left in place (and reported
  // in ReplayResult) for post-mortem inspection if replay diverges.
  std::filesystem::path replayOutputDir;
};

struct ReplayResult {
  bool ok = false;
  std::uint64_t recordsChecked = 0;
  std::optional<std::uint64_t> firstDivergentSequence;
  std::string message;  // human-readable summary, success or failure
  std::filesystem::path replayOutputDir;
};

// specification.md §11: replays every committed input in
// config.dataDir's journal through `stateMachine` (a fresh instance —
// the caller's responsibility), writes the results to a new journal at
// config.replayOutputDir, and compares it record-by-record,
// byte-for-byte against the original.
ReplayResult runReplayCheck(const ReplayConfig& config, StateMachine& stateMachine);

}  // namespace sequencer::replay::detail
