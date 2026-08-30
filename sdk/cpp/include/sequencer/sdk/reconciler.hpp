#pragma once

// specification.md §7.4: "reconcile acknowledgements against the
// published journal routinely — an O(1) check per record." A colocated
// reader (specification.md §3's "colocated consumers may instead
// memory-map the journal file directly" — the same allowance
// gateway/output and evidence/'s signing gateway both rely on) that
// compares a caller-supplied (sequenceNumber, rawRecordBytes) pair
// against what the journal actually holds. Never trusts the journal to
// say what the caller *should* have submitted — only whether what's
// published matches what the caller already independently retained,
// per §7.4: "verify... against locally retained submitted bytes, never
// against anything the venue echoes back."

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <map>
#include <optional>
#include <vector>

#include <sequencer/journal/reader.hpp>
#include <sequencer/payload.hpp>

namespace sequencer::sdk {

struct ReconciliationMismatch {
  enum class Kind {
    kNotYetCommitted,  // not an error by itself — just not there yet
    kBytesDiffer,       // specification.md §7.3: a self-contained fraud proof
  };
  std::uint64_t sequenceNumber;
  Kind kind;
};

class Reconciler {
 public:
  explicit Reconciler(const std::filesystem::path& dataDir)
      : reader_(dataDir / "journal") {}

  std::optional<ReconciliationMismatch> check(std::uint64_t sequenceNumber,
                                               Payload expectedRawRecordBytes) const {
    if (!reader_.contains(sequenceNumber)) {
      return ReconciliationMismatch{sequenceNumber, ReconciliationMismatch::Kind::kNotYetCommitted};
    }
    const Payload actual = reader_.record(sequenceNumber).rawBytes();
    if (actual.size() != expectedRawRecordBytes.size() ||
        !std::equal(actual.begin(), actual.end(), expectedRawRecordBytes.begin())) {
      return ReconciliationMismatch{sequenceNumber, ReconciliationMismatch::Kind::kBytesDiffer};
    }
    return std::nullopt;
  }

  // Batch convenience: every entry the caller retained (sequenceNumber
  // -> its own rawRecordBytes reconstruction), returning only the
  // mismatches actually found.
  std::vector<ReconciliationMismatch> checkAll(const std::map<std::uint64_t, Bytes>& retained) const {
    std::vector<ReconciliationMismatch> mismatches;
    for (const auto& [sequenceNumber, rawRecordBytes] : retained) {
      const Payload expected(rawRecordBytes.data(), rawRecordBytes.size());
      if (const std::optional<ReconciliationMismatch> mismatch = check(sequenceNumber, expected)) {
        mismatches.push_back(*mismatch);
      }
    }
    return mismatches;
  }

 private:
  journal::JournalReader reader_;
};

}  // namespace sequencer::sdk
