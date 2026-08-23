#pragma once

// JournalWriter — the single writer of one node's journal (§5, §6.3).
// Never shared between threads: specification.md §5.1 pins exactly one
// apply thread per node, and that thread is this writer's only caller.

#include <cstdint>
#include <filesystem>
#include <span>
#include <stdexcept>

#include <sequencer/journal/format.hpp>
#include <sequencer/journal/mapped_file.hpp>
#include <sequencer/journal/record_view.hpp>

namespace sequencer::journal {

struct JournalOptions {
  // Upper bounds reserved as sparse files at creation time (mapped_file.hpp:
  // one mmap call, sized once, never remapped). Actual disk usage tracks
  // only what is written, not these limits — see mapped_file.hpp's class
  // comment for why a fixed reservation was chosen over a remap strategy.
  std::uint64_t maxDataFileBytes = std::uint64_t{1} << 30;  // 1 GiB
  std::uint64_t maxIndexEntries = std::uint64_t{1} << 20;   // ~1M records
};

class JournalWriter {
 public:
  JournalWriter(const std::filesystem::path& dataPath, const std::filesystem::path& indexPath,
                JournalOptions options = {}) {
    const bool dataExists = std::filesystem::exists(dataPath);
    const bool indexExists = std::filesystem::exists(indexPath);
    if (dataExists != indexExists) {
      throw JournalFormatError("journal file pair is inconsistent: exactly one of " +
                                dataPath.string() + " / " + indexPath.string() + " exists");
    }

    if (!dataExists) {
      createNew(dataPath, indexPath, options);
    } else {
      openExisting(dataPath, indexPath);
    }
  }

  ~JournalWriter() {
    // Convenience for tooling only (§6.3) — not required for
    // correctness. The committed-count protocol alone makes the journal
    // safe to reopen after an unclean exit.
    header().closedCleanly.store(1, std::memory_order_relaxed);
    dataFile_.flush(/*async=*/false);
    indexFile_.flush(/*async=*/false);
  }

  JournalWriter(const JournalWriter&) = delete;
  JournalWriter& operator=(const JournalWriter&) = delete;

  // The sequence number append() expects next — on a fresh journal, 1;
  // on a recovered one, one past the last durably committed record
  // (§6.3). The harness (node/, not yet built) is the sole minter of
  // sequence numbers and reads this once at startup to resume.
  std::uint64_t nextSequenceNumber() const noexcept { return committedCount_ + 1; }

  // Appends one record for `sequenceNumber` (which must equal
  // nextSequenceNumber() — the journal defends the dense, gap-free
  // sequence guarantee, §2.1, at the one place a caller bug would
  // otherwise silently corrupt it) and its outputs, following the
  // three-step protocol of §6.3 exactly: data bytes, then index entry,
  // then the release-store that publishes both.
  void append(std::uint64_t sequenceNumber, Payload input, std::span<const Payload> outputs) {
    if (sequenceNumber != nextSequenceNumber()) {
      throw std::logic_error("JournalWriter::append: expected sequence number " +
                              std::to_string(nextSequenceNumber()) + ", got " +
                              std::to_string(sequenceNumber));
    }
    if (committedCount_ >= maxIndexEntries_) {
      throw std::length_error("JournalWriter::append: index file exhausted (maxIndexEntries)");
    }

    const std::size_t size = recordEncodedSize(input, outputs);
    if (nextDataOffset_ + size > dataFile_.size()) {
      throw std::length_error("JournalWriter::append: data file exhausted (maxDataFileBytes)");
    }

    // Step 1 (§6.3): write the record's bytes into the data file.
    encodeRecord(dataFile_.data() + nextDataOffset_, sequenceNumber, input, outputs);

    // Step 2: write the corresponding IndexEntry.
    indexEntries()[committedCount_] =
        IndexEntry{.byteOffset = nextDataOffset_, .entryLength = static_cast<std::uint32_t>(size),
                   .reserved = 0};

    // Step 3: release-store the new committed count. This is the single
    // synchronization point with every reader (§6.3) — everything above
    // must be program-order-before it.
    header().committedCount.store(committedCount_ + 1, std::memory_order_release);

    nextDataOffset_ += size;
    committedCount_ += 1;
  }

  void flush(bool async = true) {
    dataFile_.flush(async);
    indexFile_.flush(async);
  }

 private:
  void createNew(const std::filesystem::path& dataPath, const std::filesystem::path& indexPath,
                  const JournalOptions& options) {
    maxIndexEntries_ = options.maxIndexEntries;
    const std::size_t indexBytes =
        sizeof(IndexHeader) + static_cast<std::size_t>(options.maxIndexEntries) * sizeof(IndexEntry);

    dataFile_ = MappedFile::createNew(dataPath, options.maxDataFileBytes);
    indexFile_ = MappedFile::createNew(indexPath, indexBytes);

    // Placement-new fully and correctly starts this object's lifetime
    // for *this* process (see reader.hpp / recovery for the pragmatic
    // caveat when a different process later reinterprets these same
    // mapped bytes — the well-understood gap every lock-free
    // shared-memory format lives with).
    new (indexFile_.data()) IndexHeader{
        .magic = kIndexMagic, .version = kIndexVersion, .closedCleanly = 0, .reserved = 0,
        .committedCount = 0};

    committedCount_ = 0;
    nextDataOffset_ = 0;
  }

  void openExisting(const std::filesystem::path& dataPath, const std::filesystem::path& indexPath) {
    dataFile_ = MappedFile::openExisting(dataPath, /*readOnly=*/false);
    indexFile_ = MappedFile::openExisting(indexPath, /*readOnly=*/false);

    if (indexFile_.size() < sizeof(IndexHeader)) {
      throw JournalFormatError("index file smaller than IndexHeader: " + indexPath.string());
    }

    const IndexHeader& hdr = header();
    if (hdr.magic != kIndexMagic) {
      throw JournalFormatError("bad magic in " + indexPath.string());
    }
    if (hdr.version != kIndexVersion) {
      throw JournalFormatError("unsupported index version in " + indexPath.string());
    }

    maxIndexEntries_ = (indexFile_.size() - sizeof(IndexHeader)) / sizeof(IndexEntry);

    // §6.3 recovery: the committed count alone is authoritative. A
    // record written but never published (a crash between step 1 and
    // step 3) is simply absent from this count and will be silently
    // overwritten by the next append() — nothing to detect or repair.
    committedCount_ = hdr.committedCount.load(std::memory_order_acquire);

    if (committedCount_ == 0) {
      nextDataOffset_ = 0;
    } else {
      const IndexEntry& last = indexEntries()[committedCount_ - 1];
      nextDataOffset_ = last.byteOffset + last.entryLength;
    }
  }

  IndexHeader& header() noexcept { return *reinterpret_cast<IndexHeader*>(indexFile_.data()); }
  const IndexHeader& header() const noexcept {
    return *reinterpret_cast<const IndexHeader*>(indexFile_.data());
  }
  IndexEntry* indexEntries() noexcept {
    return reinterpret_cast<IndexEntry*>(indexFile_.data() + sizeof(IndexHeader));
  }

  MappedFile dataFile_;
  MappedFile indexFile_;
  std::uint64_t committedCount_ = 0;
  std::uint64_t nextDataOffset_ = 0;
  std::uint64_t maxIndexEntries_ = 0;
};

}  // namespace sequencer::journal
