#include "replay_check.hpp"

#include <sequencer/temp_dir.hpp>
#include <sequencer/journal/reader.hpp>
#include <sequencer/journal/writer.hpp>

#include <algorithm>
#include <sstream>
#include <stdexcept>

namespace sequencer::replay::detail {

namespace {


}  // namespace

ReplayResult runReplayCheck(const ReplayConfig& config, StateMachine& stateMachine) {
  ReplayResult result;

  journal::JournalReader original(config.dataDir / "journal");
  const std::uint64_t total = original.committedCount();

  const bool autoCreatedOutputDir = config.replayOutputDir.empty();
  result.replayOutputDir = autoCreatedOutputDir ? sequencer::makeTempDir("sequencer_replay") : config.replayOutputDir;
  if (!autoCreatedOutputDir) {
    std::filesystem::create_directories(result.replayOutputDir);
  }

  // Match the original journal's geometry rather than trusting some
  // unrelated default (specification.md §5.3-style defaults are a
  // node's concern, not this tool's). Since §6.5 the geometry is a
  // property of the journal being replayed and is readable from it, so
  // this no longer has to infer sizes from file footprints: replaying
  // into the same shape keeps the comparison honest and keeps a replay
  // rolling at exactly the points the original did.
  journal::JournalOptions options;
  options.recordsPerSegment = original.recordsPerSegment();
  options.maxRecordBytes = original.maxRecordBytes();

  {
    journal::JournalWriter replayWriter(result.replayOutputDir / "journal", options);
    OutputCollector collector;
    for (std::uint64_t seq = 1; seq <= total; ++seq) {
      const journal::RecordView originalRecord = original.record(seq);
      collector.reset();
      // The whole point: apply() runs fresh, on the caller's state
      // machine instance, exactly as the pinned apply thread would —
      // determinism means this must produce the identical outputs
      // (specification.md §4.1).
      stateMachine.apply(seq, originalRecord.input(), collector);
      replayWriter.append(seq, originalRecord.input(), collector.outputs());
    }
    replayWriter.flush(/*async=*/false);
  }

  journal::JournalReader replayed(result.replayOutputDir / "journal");
  if (replayed.committedCount() != total) {
    result.ok = false;
    std::ostringstream msg;
    msg << "replay produced a different record count: original=" << total
        << " replayed=" << replayed.committedCount();
    result.message = msg.str();
    return result;
  }

  for (std::uint64_t seq = 1; seq <= total; ++seq) {
    const Payload a = original.record(seq).rawBytes();
    const Payload b = replayed.record(seq).rawBytes();
    const bool same = a.size() == b.size() && std::equal(a.begin(), a.end(), b.begin());
    if (!same) {
      result.ok = false;
      result.firstDivergentSequence = seq;
      std::ostringstream msg;
      msg << "journal diverged at sequence number " << seq << ": original record is " << a.size()
          << " bytes, replayed record is " << b.size() << " bytes";
      result.message = msg.str();
      return result;
    }
  }

  result.ok = true;
  result.recordsChecked = total;
  std::ostringstream msg;
  msg << "replay OK: " << total << " record(s) byte-identical";
  result.message = msg.str();

  if (autoCreatedOutputDir) {
    std::filesystem::remove_all(result.replayOutputDir);
  }
  return result;
}

}  // namespace sequencer::replay::detail
