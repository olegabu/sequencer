#include "replay_check.hpp"

#include <sequencer/journal/reader.hpp>
#include <sequencer/journal/writer.hpp>

#include <algorithm>
#include <sstream>
#include <stdexcept>

namespace sequencer::replay::detail {

namespace {

std::filesystem::path makeTempDir() {
  std::string tmpl = (std::filesystem::temp_directory_path() / "sequencer_replay_XXXXXX").string();
  if (::mkdtemp(tmpl.data()) == nullptr) {
    throw std::runtime_error("replay: mkdtemp failed");
  }
  return tmpl;
}

}  // namespace

ReplayResult runReplayCheck(const ReplayConfig& config, StateMachine& stateMachine) {
  ReplayResult result;

  journal::JournalReader original(config.dataDir / "journal.data", config.dataDir / "journal.index");
  const std::uint64_t total = original.committedCount();

  const bool autoCreatedOutputDir = config.replayOutputDir.empty();
  result.replayOutputDir = autoCreatedOutputDir ? makeTempDir() : config.replayOutputDir;
  if (!autoCreatedOutputDir) {
    std::filesystem::create_directories(result.replayOutputDir);
  }

  // Size the replay journal from the original's own footprint, rather
  // than trusting some unrelated default (specification.md §5.3-style
  // defaults are a node's concern, not this tool's).
  journal::JournalOptions options;
  options.maxIndexEntries = std::max<std::uint64_t>(total, 1) * 2;
  const std::uint64_t originalDataBytes = std::filesystem::exists(config.dataDir / "journal.data")
                                               ? std::filesystem::file_size(config.dataDir / "journal.data")
                                               : 0;
  options.maxDataFileBytes = std::max<std::uint64_t>(originalDataBytes * 2, std::uint64_t{1} << 20);

  {
    journal::JournalWriter replayWriter(result.replayOutputDir / "journal.data",
                                         result.replayOutputDir / "journal.index", options);
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

  journal::JournalReader replayed(result.replayOutputDir / "journal.data",
                                   result.replayOutputDir / "journal.index");
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
