#pragma once

// JournalReader — any number of independent, concurrent readers over one
// journal (§6.4). Colocated readers memory-map the files directly, as
// this class does; remote readers reach the same bytes through
// Subscribe, served by a node or a relay gateway (§3, not yet built).

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <stdexcept>
#include <thread>

#include <sequencer/journal/format.hpp>
#include <sequencer/journal/mapped_file.hpp>
#include <sequencer/journal/record_view.hpp>

namespace sequencer::journal {

class JournalReader {
 public:
  JournalReader(const std::filesystem::path& dataPath, const std::filesystem::path& indexPath) {
    dataFile_ = MappedFile::openExisting(dataPath, /*readOnly=*/true);
    indexFile_ = MappedFile::openExisting(indexPath, /*readOnly=*/true);

    if (indexFile_.size() < sizeof(IndexHeader)) {
      throw JournalFormatError("index file smaller than IndexHeader: " + indexPath.string());
    }
    if (header().magic != kIndexMagic) {
      throw JournalFormatError("bad magic in " + indexPath.string());
    }
    if (header().version != kIndexVersion) {
      throw JournalFormatError("unsupported index version in " + indexPath.string());
    }
  }

  // The only synchronization in the journal (§6.3): an acquire-load
  // paired with the writer's release-store. Every entry below the
  // returned value is safe to read in full.
  std::uint64_t committedCount() const noexcept {
    return header().committedCount.load(std::memory_order_acquire);
  }

  bool contains(std::uint64_t sequenceNumber) const noexcept {
    return sequenceNumber >= 1 && sequenceNumber <= committedCount();
  }

  // O(1): a single index-array lookup (§6.1). `sequenceNumber` must be
  // in [1, committedCount()].
  RecordView record(std::uint64_t sequenceNumber) const {
    if (!contains(sequenceNumber)) {
      throw std::out_of_range("JournalReader::record: sequence number " +
                               std::to_string(sequenceNumber) + " not committed (committedCount=" +
                               std::to_string(committedCount()) + ")");
    }
    const IndexEntry& entry = indexEntries()[sequenceNumber - 1];
    return RecordView(dataFile_.data() + entry.byteOffset, entry.entryLength);
  }

  // Blocks (§6.4: "spinning briefly on the committed count, then
  // backing off") until `sequenceNumber` is committed, then returns it.
  // Pass pureSpin=true for a latency-sensitive colocated reader that
  // would rather burn a core than pay a sleep's wakeup latency.
  RecordView waitForRecord(std::uint64_t sequenceNumber, bool pureSpin = false) const {
    constexpr int kSpinIterations = 1000;
    int spins = 0;
    while (!contains(sequenceNumber)) {
      if (pureSpin || spins++ < kSpinIterations) {
        // A brief busy-spin before backing off, so a record that
        // commits within microseconds is observed without paying a
        // sleep's wakeup latency.
        continue;
      }
      std::this_thread::sleep_for(std::chrono::microseconds(50));
    }
    return record(sequenceNumber);
  }

 private:
  const IndexHeader& header() const noexcept {
    return *reinterpret_cast<const IndexHeader*>(indexFile_.data());
  }
  const IndexEntry* indexEntries() const noexcept {
    return reinterpret_cast<const IndexEntry*>(indexFile_.data() + sizeof(IndexHeader));
  }

  MappedFile dataFile_;
  MappedFile indexFile_;
};

}  // namespace sequencer::journal
