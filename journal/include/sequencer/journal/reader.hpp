#pragma once

// JournalReader — any number of independent, concurrent readers over one
// journal (§6.4, §6.5). Colocated readers memory-map the files directly,
// as this class does; remote readers reach the same bytes through
// Subscribe, served by a node or a relay gateway (§3).
//
// One instance is safe to share across threads, and is meant to be:
// gateway/relay hands a single shared_ptr<JournalReader> to every
// session thread. That is why the segment cache below is lock-free
// rather than mutex-guarded — putting a lock on record() would serialize
// exactly the path the relay exists to keep fast.

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstdio>
#include <filesystem>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>

#include <sequencer/journal/format.hpp>
#include <sequencer/journal/mapped_file.hpp>
#include <sequencer/journal/record_view.hpp>

namespace sequencer::journal {

class JournalReader {
 public:
  // A reader maps at most this many segments. Not a limit on how large
  // a journal may grow -- only on how much of one a SINGLE reader may
  // touch in its lifetime, since mappings here are never released.
  //
  // They are never released because a RecordView is a bare pointer into
  // a mapping, and this reader is shared across threads: unmapping a
  // segment one thread has finished with could pull the ground out from
  // under a view another thread still holds. Segments are immutable once
  // sealed, so holding a mapping costs address space and nothing else.
  //
  // At the default geometry (1.05M records per segment) this is 1.07B
  // records for one reader. A process that must stream past that today
  // constructs a fresh JournalReader and drops the old one; making a
  // reader recycle mappings safely needs a lifetime contract on
  // RecordView that it does not currently have.
  static constexpr std::size_t kMaxMappedSegments = 1024;

  // `dir` is the journal directory (§6.5): a manifest plus segments.
  //
  // Only the manifest is validated here. Segments are opened and
  // checked on first read, because a reader that validated every
  // segment up front would have to map every segment up front, which is
  // the opposite of what §6.5 is for -- a journal may hold thousands and
  // a tailing reader touches the last one. So a corrupt segment surfaces
  // from record(), not from the constructor.
  explicit JournalReader(const std::filesystem::path& dir) : dir_(dir) {
    const std::filesystem::path manifestPath = dir_ / kManifestFileName;
    manifestFile_ = MappedFile::openExisting(manifestPath, /*readOnly=*/true);

    if (manifestFile_.size() < sizeof(JournalManifest)) {
      throw JournalFormatError("manifest smaller than JournalManifest: " + manifestPath.string());
    }
    if (manifest().magic != kManifestMagic) {
      throw JournalFormatError("bad magic in " + manifestPath.string());
    }
    if (manifest().version != kManifestVersion) {
      throw JournalFormatError("unsupported manifest version in " + manifestPath.string());
    }
    recordsPerSegment_ = manifest().recordsPerSegment;
    if (recordsPerSegment_ == 0) {
      throw JournalFormatError("manifest declares recordsPerSegment=0: " + manifestPath.string());
    }
    for (auto& slot : segments_) {
      slot.store(nullptr, std::memory_order_relaxed);
    }
  }

  ~JournalReader() {
    for (auto& slot : segments_) {
      delete slot.load(std::memory_order_relaxed);
    }
  }

  JournalReader(const JournalReader&) = delete;
  JournalReader& operator=(const JournalReader&) = delete;

  // The only synchronization in the journal (§6.3): an acquire-load
  // paired with the writer's release-store. Every entry below the
  // returned value is safe to read in full — including, since §6.5, the
  // existence of the segment holding it, because the writer creates a
  // segment before publishing any record in it.
  std::uint64_t committedCount() const noexcept {
    return manifest().committedCount.load(std::memory_order_acquire);
  }

  bool contains(std::uint64_t sequenceNumber) const noexcept {
    return sequenceNumber >= 1 && sequenceNumber <= committedCount();
  }

  // O(1): a division to find the segment and a single index-array lookup
  // within it (§6.1, §6.5). `sequenceNumber` must be in
  // [1, committedCount()].
  RecordView record(std::uint64_t sequenceNumber) const {
    if (!contains(sequenceNumber)) {
      throw std::out_of_range("JournalReader::record: sequence number " +
                               std::to_string(sequenceNumber) + " not committed (committedCount=" +
                               std::to_string(committedCount()) + ")");
    }
    const Segment& segment = segmentFor((sequenceNumber - 1) / recordsPerSegment_);
    const IndexEntry& entry =
        reinterpret_cast<const IndexEntry*>(segment.index.data() +
                                             sizeof(IndexHeader))[(sequenceNumber - 1) %
                                                                   recordsPerSegment_];
    return RecordView(segment.data.data() + entry.byteOffset, entry.entryLength);
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

  // The geometry this journal was created with (§6.5). Exposed so a
  // tool that writes a second journal from this one -- tools/replay --
  // can match it rather than guess, which keeps a replay rolling at
  // exactly the points the original did.
  std::uint64_t recordsPerSegment() const noexcept { return recordsPerSegment_; }
  std::uint64_t maxRecordBytes() const noexcept { return manifest().maxRecordBytes; }

 private:
  struct Segment {
    MappedFile data;
    MappedFile index;
  };

  // Lock-free, install-once cache. A thread that finds an empty slot
  // maps the segment and tries to publish it; if another thread won the
  // race, it drops its own copy and uses the winner's. Every later read
  // of an already-mapped segment is one acquire-load.
  const Segment& segmentFor(std::uint64_t segment) const {
    if (segment >= kMaxMappedSegments) {
      throw std::out_of_range(
          "JournalReader: segment " + std::to_string(segment) + " is beyond the " +
          std::to_string(kMaxMappedSegments) +
          " a single reader may map. Mappings are never released because a RecordView "
          "points into one and this reader may be shared across threads; construct a "
          "fresh JournalReader to read further.");
    }
    std::atomic<Segment*>& slot = segments_[segment];
    if (Segment* mapped = slot.load(std::memory_order_acquire); mapped != nullptr) {
      return *mapped;
    }

    auto opened = std::make_unique<Segment>(openSegment(segment));
    Segment* expected = nullptr;
    if (slot.compare_exchange_strong(expected, opened.get(), std::memory_order_acq_rel,
                                      std::memory_order_acquire)) {
      return *opened.release();
    }
    // Lost the race; `expected` now holds the winner, and our own
    // mapping is dropped as `opened` goes out of scope.
    return *expected;
  }

  // Both candidate names are derivable from the segment index (§6.5), so
  // this needs no directory listing: try the sealed name, then the open
  // one.
  //
  // A reader can lose a race with the writer's seal-by-rename and find
  // neither name for an instant. It retries rather than failing — the
  // segment did not go anywhere, and a record whose existence the
  // committed count already promised cannot become unreadable.
  Segment openSegment(std::uint64_t segment) const {
    // Timed because this is the reader's counterpart to the writer's
    // roll, and the writer's roll turned out to own the tail. A tailing
    // gateway must open and map each new segment as the writer reaches
    // it -- exists() probes, two mmaps, and on a lost race a retry loop
    // -- and it does so on the very thread that delivers records. Off
    // unless SEQ_SEGMENT_OPEN_US is set; off reads no clock.
    static const char* const kEnv = std::getenv("SEQ_SEGMENT_OPEN_US");
    static const std::int64_t kThresholdUs = kEnv != nullptr ? std::atoll(kEnv) : 0;
    const auto openStart = kThresholdUs > 0 ? std::chrono::steady_clock::now()
                                            : std::chrono::steady_clock::time_point{};
    struct Report {
      const std::int64_t threshold;
      const std::chrono::steady_clock::time_point start;
      std::uint64_t segment;
      int attempts = 0;
      ~Report() {
        if (threshold <= 0) {
          return;
        }
        const auto us = std::chrono::duration_cast<std::chrono::microseconds>(
                             std::chrono::steady_clock::now() - start)
                             .count();
        if (us >= threshold) {
          const auto wall = std::chrono::duration_cast<std::chrono::microseconds>(
                                 std::chrono::system_clock::now().time_since_epoch())
                                 .count();
          std::fprintf(stderr,
                       "[segment-open] t=%lld.%06lld segment=%llu attempts=%d took=%lldus\n",
                       static_cast<long long>(wall / 1000000),
                       static_cast<long long>(wall % 1000000),
                       static_cast<unsigned long long>(segment), attempts,
                       static_cast<long long>(us));
        }
      }
    } report{kThresholdUs, openStart, segment, 0};

    const std::uint64_t first = segment * recordsPerSegment_ + 1;
    const std::string sealed = sealedSegmentStem(first, first + recordsPerSegment_ - 1);
    const std::string open = openSegmentStem(first);

    constexpr int kAttempts = 100;
    for (int attempt = 0; attempt < kAttempts; ++attempt) {
      report.attempts = attempt + 1;
      bool isSealed = true;
      for (const std::string& stem : {sealed, open}) {
        const bool sealedName = isSealed;
        isSealed = false;
        const std::filesystem::path dataPath = dir_ / (stem + ".data");
        const std::filesystem::path indexPath = dir_ / (stem + ".index");
        if (!std::filesystem::exists(dataPath) || !std::filesystem::exists(indexPath)) {
          continue;
        }
        Segment result;
        try {
          result.index = MappedFile::openExisting(indexPath, /*readOnly=*/true);
          if (result.index.size() < sizeof(IndexHeader)) {
            throw JournalFormatError("index file smaller than IndexHeader: " + indexPath.string());
          }
          // A sealed segment's data is complete and its length exactly
          // known from the last index entry, so map that rather than the
          // whole reservation — see MappedFile::openExisting's mapAtMost.
          // The active segment is still growing, so it gets the full
          // reservation; there is only ever one of those per reader.
          std::size_t mapAtMost = 0;
          if (sealedName) {
            const auto* entries =
                reinterpret_cast<const IndexEntry*>(result.index.data() + sizeof(IndexHeader));
            const IndexEntry& last = entries[recordsPerSegment_ - 1];
            mapAtMost = static_cast<std::size_t>(last.byteOffset) + last.entryLength;
          }
          result.data = MappedFile::openExisting(dataPath, /*readOnly=*/true, mapAtMost);
        } catch (const std::system_error&) {
          // Renamed between the exists() check and the open. Fall
          // through and try the other name, or retry.
          continue;
        }
        const auto& hdr = *reinterpret_cast<const IndexHeader*>(result.index.data());
        if (hdr.magic != kIndexMagic) {
          throw JournalFormatError("bad magic in " + indexPath.string());
        }
        if (hdr.version != kIndexVersion) {
          throw JournalFormatError("unsupported index version in " + indexPath.string());
        }
        return result;
      }
      std::this_thread::sleep_for(std::chrono::microseconds(50));
    }
    throw JournalFormatError("JournalReader: neither " + sealed + " nor " + open +
                              " exists in " + dir_.string() +
                              " — the committed count promised a record in this segment");
  }

  const JournalManifest& manifest() const noexcept {
    return *reinterpret_cast<const JournalManifest*>(manifestFile_.data());
  }

  std::filesystem::path dir_;
  MappedFile manifestFile_;
  std::uint64_t recordsPerSegment_ = 0;
  mutable std::atomic<Segment*> segments_[kMaxMappedSegments];
};

}  // namespace sequencer::journal
