#pragma once

// JournalWriter — the single writer of one node's journal (§5, §6.3,
// §6.5). Never shared between threads: specification.md §5.1 pins
// exactly one apply thread per node, and that thread is this writer's
// only caller. (fillPercent() is the one deliberate exception; see it.)
//
// A journal is a DIRECTORY (§6.5): a small `manifest` holding the
// geometry and the committed count, plus a series of segment file pairs.
// The writer appends into the active segment and starts a new one when
// that segment's record count is reached.

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <mutex>
#include <thread>
#include <filesystem>
#include <span>
#include <stdexcept>
#include <string>
#include <system_error>

#include <sequencer/journal/format.hpp>
#include <sequencer/journal/mapped_file.hpp>
#include <sequencer/journal/record_view.hpp>

namespace sequencer::journal {

// Thrown by append() when a record cannot be written. Its own type
// rather than a bare std::length_error so a caller can tell "this
// journal cannot take this record" -- terminal for the node but not a
// bug -- apart from the std::logic_error a sequence-number violation
// raises, which is. Before this existed, both reached the apply thread
// as an unhandled throw and took the process down through
// std::terminate, with only "terminate called after throwing an
// instance of 'std::length_error'" to go on; that cost two benchmark
// fleets before it was diagnosed.
class JournalExhausted : public std::length_error {
 public:
  using std::length_error::length_error;
};

struct JournalOptions {
  // §6.5's geometry, fixed when a journal is created and never changed
  // afterwards. The two multiply: a segment's data file is reserved at
  // recordsPerSegment * maxRecordBytes, so that the RECORD COUNT is
  // always what ends a segment and never the data running out. That is
  // what keeps addressing a division rather than a table lookup --
  // segment = (K-1)/recordsPerSegment, slot = (K-1)%recordsPerSegment --
  // which §6.1 promises and every reader depends on.
  //
  // So raising maxRecordBytes multiplies the per-segment reservation.
  // The defaults reserve 256 GiB of data and 16 MiB of index per
  // segment. Neither costs what it looks like: the files are sparse
  // (mapped_file.hpp), so they use no disk until written, and a reader
  // maps a SEALED segment at its used length rather than its reserved
  // one (reader.hpp), so address space tracks data rather than
  // reservation. 256 KiB also comfortably exceeds a node's own default
  // maxInputSize of 64 KiB (§5.3), so the journal is not what bounds a
  // record first.
  //
  // A record larger than maxRecordBytes is refused, not rolled early --
  // rolling early would break the arithmetic above. An application with
  // genuinely larger records raises this and lowers recordsPerSegment to
  // keep the product sane.
  std::uint64_t recordsPerSegment = std::uint64_t{1} << 20;   // ~1.05M records
  std::uint64_t maxRecordBytes = std::uint64_t{1} << 18;      // 256 KiB
};

class JournalWriter {
 public:
  // `dir` is the journal directory. Created along with its first
  // segment if absent; otherwise reopened, taking its geometry from the
  // manifest rather than from `options` (geometry is immutable once a
  // journal exists, since every segment already written depends on it).
  explicit JournalWriter(const std::filesystem::path& dir, JournalOptions options = {})
      : dir_(dir) {
    const std::filesystem::path manifestPath = dir_ / kManifestFileName;
    if (std::filesystem::exists(manifestPath)) {
      openExisting(manifestPath);
    } else {
      createNew(manifestPath, options);
    }
    startWorker();
  }

  ~JournalWriter() {
    // Drain first: a seal still queued means a filled segment is on disk
    // under its in-progress name. Readers resolve that name, so it is
    // not corruption, but leaving it that way across a restart would
    // hand the next process a journal whose segments are inconsistently
    // named for no reason.
    drainSeals();
    stopWorker();

    // Convenience for tooling only (§6.3) -- not required for
    // correctness. The committed-count protocol alone makes the journal
    // safe to reopen after an unclean exit.
    manifest().closedCleanly.store(1, std::memory_order_relaxed);
    manifestFile_.flush(/*async=*/false);
    dataFile_.flush(/*async=*/false);
    indexFile_.flush(/*async=*/false);
  }

  JournalWriter(const JournalWriter&) = delete;
  JournalWriter& operator=(const JournalWriter&) = delete;

  // The sequence number append() expects next — on a fresh journal, 1;
  // on a recovered one, one past the last durably committed record
  // (§6.3).
  std::uint64_t nextSequenceNumber() const noexcept { return committedCount_ + 1; }

  // Appends one record for `sequenceNumber` (which must equal
  // nextSequenceNumber() — the journal defends the dense, gap-free
  // sequence guarantee, §2.1, at the one place a caller bug would
  // otherwise silently corrupt it) and its outputs, following the
  // three-step protocol of §6.3 exactly: data bytes, then index entry,
  // then the release-store that publishes both.
  //
  // §6.5 adds a step 0 before those three, on the record that begins a
  // new segment: seal the active segment and open the next one. The
  // ordering is what makes rollover safe for readers — see roll().
  void append(std::uint64_t sequenceNumber, Payload input, std::span<const Payload> outputs) {
    if (sequenceNumber != nextSequenceNumber()) {
      throw std::logic_error("JournalWriter::append: expected sequence number " +
                              std::to_string(nextSequenceNumber()) + ", got " +
                              std::to_string(sequenceNumber));
    }

    const std::size_t size = recordEncodedSize(input, outputs);
    if (size > maxRecordBytes_) {
      throw JournalExhausted(
          "JournalWriter::append: record of " + std::to_string(size) +
          " bytes exceeds maxRecordBytes (" + std::to_string(maxRecordBytes_) +
          "). This bound is fixed when a journal is created, because a segment's data file "
          "is reserved at recordsPerSegment * maxRecordBytes so that the record count "
          "always ends a segment first (specification.md 6.5). Create the journal with a "
          "larger JournalOptions::maxRecordBytes -- and a correspondingly smaller "
          "recordsPerSegment -- rather than raising it on an existing one.");
    }

    // Step 0 (§6.5): the record that starts a new segment rolls first.
    if (slotFor(sequenceNumber) == 0 && sequenceNumber != 1) {
      roll(sequenceNumber);
    }

    // Step 1 (§6.3): write the record's bytes into the data file.
    encodeRecord(dataFile_.data() + nextDataOffset_, sequenceNumber, input, outputs);

    // Step 2: write the corresponding IndexEntry, at this record's slot
    // WITHIN its segment.
    indexEntries()[slotFor(sequenceNumber)] =
        IndexEntry{.byteOffset = nextDataOffset_, .entryLength = static_cast<std::uint32_t>(size),
                   .reserved = 0};

    // Step 3: release-store the new committed count. This is the single
    // synchronization point with every reader (§6.3) — everything above
    // must be program-order-before it, which now includes the segment
    // creation in step 0.
    manifest().committedCount.store(committedCount_ + 1, std::memory_order_release);

    nextDataOffset_ += size;
    committedCount_ += 1;
    // For fillPercent() only, which a monitoring thread may call.
    publishedDataOffset_.store(nextDataOffset_, std::memory_order_relaxed);

    // Start building the next segment well before the roll needs it, so
    // roll() finds it already mapped. Integer compare per record; the
    // lock is taken only on the one record that crosses the threshold.
    const std::uint64_t slot = slotFor(sequenceNumber);
    if (recordsPerSegment_ > 0 && slot == recordsPerSegment_ * kPrepareAtPercent / 100) {
      std::lock_guard<std::mutex> lock(workMutex_);
      requestPrepareLocked(activeSegment_ + 1);
    }
  }

  // How full the ACTIVE segment is, as a percentage of whichever of its
  // two reservations is closer to full. Since §6.5 rolls, this is no
  // longer a countdown to failure the way it was when a journal was one
  // fixed pair — it is a sawtooth, resetting on every roll. It is still
  // worth publishing (the node exposes it as journal_fill_percent),
  // because a segment whose data never approaches full means the
  // geometry is reserving far more per segment than it needs.
  //
  // THE ONE MEMBER SAFE TO CALL FROM ANOTHER THREAD. Everything else
  // here belongs to the single apply thread (see this file's header
  // comment). This reads only atomics: the manifest's committed count,
  // which append() already publishes with release semantics, and a
  // mirror of the data offset that append() stores alongside it. Both
  // may be a record stale, which is immaterial for a fill gauge.
  int fillPercent() const noexcept {
    const std::uint64_t committed = manifest().committedCount.load(std::memory_order_acquire);
    const std::uint64_t slot = recordsPerSegment_ == 0 ? 0 : committed % recordsPerSegment_;
    const std::uint64_t byIndex = recordsPerSegment_ == 0 ? 100 : slot * 100 / recordsPerSegment_;
    const std::uint64_t dataSize = dataFile_.size();
    const std::uint64_t offset = publishedDataOffset_.load(std::memory_order_relaxed);
    const std::uint64_t byData = dataSize == 0 ? 100 : offset * 100 / dataSize;
    return static_cast<int>(byIndex > byData ? byIndex : byData);
  }

  std::uint64_t committedCount() const noexcept { return committedCount_; }
  std::uint64_t recordsPerSegment() const noexcept { return recordsPerSegment_; }
  std::uint64_t maxRecordBytes() const noexcept { return maxRecordBytes_; }
  // How many segments this journal has written, including the active
  // one. Exposed for tests and tooling that assert rollover happened.
  std::uint64_t segmentCount() const noexcept { return activeSegment_ + 1; }

  void flush(bool async = true) {
    dataFile_.flush(async);
    indexFile_.flush(async);
    manifestFile_.flush(async);
  }

 private:
  std::uint64_t slotFor(std::uint64_t sequenceNumber) const noexcept {
    return (sequenceNumber - 1) % recordsPerSegment_;
  }
  std::uint64_t segmentFor(std::uint64_t sequenceNumber) const noexcept {
    return (sequenceNumber - 1) / recordsPerSegment_;
  }
  std::uint64_t firstOfSegment(std::uint64_t segment) const noexcept {
    return segment * recordsPerSegment_ + 1;
  }

  std::filesystem::path segmentPath(const std::string& stem, const char* suffix) const {
    return dir_ / (stem + suffix);
  }

  // Seals the active segment and opens the one `sequenceNumber` belongs
  // to.
  //
  // Sealing is a rename, exactly as braft does it (§6.5). It is safe to
  // do underneath live readers because a rename on POSIX disturbs
  // neither an open descriptor nor an existing mapping: a reader
  // already inside this segment keeps reading it, and a reader opening
  // it by name either finds the sealed name or retries.
  //
  // The new segment is created BEFORE any record in it is published,
  // which is the ordering §6.5 relies on: a reader that acquire-loads a
  // committed count reaching into this segment is guaranteed, by the
  // release-store in append(), to see everything done here first.
  // A pointer swap and two queue operations. Nothing here touches the
  // filesystem or waits on one.
  //
  // It used to do all three of the expensive things inline, on the apply
  // thread: an msync of everything dirty in the segment just filled (up
  // to recordsPerSegment records' worth of pages), two renames, then the
  // creation, sizing and mapping of the next segment's file pair. Every
  // record queued behind that waited. Once per 1,048,576 records is
  // invisible in p90 and dominates p999: measured on a five-client
  // fleet, every gateway flavour carried a 50-240ms p999 at every rate
  // from 75k up while bare braft on the same hardware stayed under 2ms,
  // and raising the segment size 16x -- making the roll 16x rarer --
  // took p999 to 3-5ms and p99 from 17-60ms to ~2ms. That is what
  // identified this function; making the roll rare was the diagnosis,
  // and taking the work off this thread is the fix.
  void roll(std::uint64_t sequenceNumber) {
    const std::uint64_t sealedFirst = firstOfSegment(activeSegment_);
    const std::uint64_t sealedLast = sealedFirst + recordsPerSegment_ - 1;
    const std::uint64_t nextSegment = segmentFor(sequenceNumber);

    std::unique_lock<std::mutex> lock(workMutex_);
    // Normally already done: preparation starts at kPrepareAtPercent of
    // the active segment. Waiting here is the fallback for a burst that
    // outruns it, and is still no worse than doing it inline was.
    if (preparedSegment_ != nextSegment) {
      requestPrepareLocked(nextSegment);
    }
    // The segment identity is part of the condition, not an assumption:
    // waking on "something is ready" would adopt a mapping for a
    // different segment as this one's.
    workCv_.wait(lock, [&] {
      return (preparedReady_ && preparedSegment_ == nextSegment) || preparedError_ != nullptr;
    });
    if (preparedError_ != nullptr) {
      std::exception_ptr e = preparedError_;
      preparedError_ = nullptr;
      preparedReady_ = false;
      preparedSegment_ = kNoSegment;
      std::rethrow_exception(e);
    }

    // The filled segment goes to the worker to be flushed and THEN
    // renamed -- that order is the durability property, not the thread
    // it runs on: a crash after the rename must not find the sealed
    // segment's bytes only in page cache. Readers tolerate the delay by
    // construction, trying the sealed name and falling back to the
    // in-progress one (reader.hpp's openSegment).
    seals_.push_back(PendingSeal{std::move(dataFile_), std::move(indexFile_), sealedFirst,
                                  sealedLast});
    sealsInFlight_ += 1;

    dataFile_ = std::move(preparedData_);
    indexFile_ = std::move(preparedIndex_);
    preparedReady_ = false;
    preparedSegment_ = kNoSegment;

    // Everything openSegment() used to reset on the writer's behalf.
    // Missing the offset let the writer keep appending at the FILLED
    // segment's offset into a fresh mapping, straight off the end of it
    // -- caught immediately by Concurrency.ReadersFollowTheWriterAcross-
    // SegmentBoundaries, which is precisely the test for this path.
    activeSegment_ = nextSegment;
    nextDataOffset_ = 0;
    publishedDataOffset_.store(0, std::memory_order_relaxed);

    lock.unlock();
    workCv_.notify_all();
  }

  // Maps segment `segment`'s file pair as the active one. `create`
  // asks for files that do not exist yet.
  //
  // Recovery of a half-rolled journal is handled here rather than
  // treated as damage: a crash between creating a segment and
  // publishing its first record leaves files holding no published
  // records, which is indistinguishable from -- and as harmless as -- a
  // crash mid-record (§6.3). If the files are already there, adopt
  // them; their contents below the committed count are authoritative
  // and anything above it will simply be overwritten.
  // Creates and maps one segment's file pair. Called either inline (the
  // first segment) or from the worker thread, ahead of the roll that
  // needs it -- which is why it takes its outputs by reference instead
  // of assigning the active mappings.
  void createSegmentFiles(std::uint64_t segment, MappedFile& data, MappedFile& index) {
    const std::string stem = openSegmentStem(firstOfSegment(segment));
    data = MappedFile::createNew(segmentPath(stem, ".data"), recordsPerSegment_ * maxRecordBytes_);
    index = MappedFile::createNew(
        segmentPath(stem, ".index"),
        sizeof(IndexHeader) + static_cast<std::size_t>(recordsPerSegment_) * sizeof(IndexEntry));
    // Placement-new fully and correctly starts this object's lifetime
    // for *this* process (see reader.hpp for the pragmatic caveat when
    // a different process later reinterprets these same mapped bytes
    // — the well-understood gap every lock-free shared-memory format
    // lives with).
    //
    // A segment's own committedCount is NOT the publication signal any
    // more (§6.5 moved that to the manifest) and stays zero; magic and
    // version remain so a segment file is still self-identifying.
    new (index.data()) IndexHeader{.magic = kIndexMagic,
                                    .version = kIndexVersion,
                                    .closedCleanly = 0,
                                    .reserved = 0,
                                    .committedCount = 0};
  }

  void openSegment(std::uint64_t segment, bool create) {
    const std::string stem = openSegmentStem(firstOfSegment(segment));
    const std::filesystem::path dataPath = segmentPath(stem, ".data");
    const std::filesystem::path indexPath = segmentPath(stem, ".index");

    const bool dataExists = std::filesystem::exists(dataPath);
    const bool indexExists = std::filesystem::exists(indexPath);
    if (dataExists != indexExists) {
      // Half a segment is not something to guess about: creating the
      // missing file would silently invent an index for data it has
      // never seen, and adopting the pair would read garbage offsets.
      throw JournalFormatError("segment file pair is inconsistent: exactly one of " +
                                dataPath.string() + " / " + indexPath.string() + " exists");
    }
    const bool present = dataExists && indexExists;
    if (create && !present) {
      createSegmentFiles(segment, dataFile_, indexFile_);
    } else {
      dataFile_ = MappedFile::openExisting(dataPath, /*readOnly=*/false);
      indexFile_ = MappedFile::openExisting(indexPath, /*readOnly=*/false);
      if (indexFile_.size() < sizeof(IndexHeader)) {
        throw JournalFormatError("index file smaller than IndexHeader: " + indexPath.string());
      }
      const IndexHeader& hdr = *reinterpret_cast<const IndexHeader*>(indexFile_.data());
      if (hdr.magic != kIndexMagic) {
        throw JournalFormatError("bad magic in " + indexPath.string());
      }
      if (hdr.version != kIndexVersion) {
        throw JournalFormatError("unsupported index version in " + indexPath.string());
      }
    }

    activeSegment_ = segment;
    nextDataOffset_ = 0;
    publishedDataOffset_.store(0, std::memory_order_relaxed);
  }

  void createNew(const std::filesystem::path& manifestPath, const JournalOptions& options) {
    if (options.recordsPerSegment == 0 || options.maxRecordBytes == 0) {
      throw std::invalid_argument(
          "JournalOptions: recordsPerSegment and maxRecordBytes must both be non-zero");
    }
    std::filesystem::create_directories(dir_);

    manifestFile_ = MappedFile::createNew(manifestPath, sizeof(JournalManifest));
    new (manifestFile_.data()) JournalManifest{.magic = kManifestMagic,
                                                .version = kManifestVersion,
                                                .recordsPerSegment = options.recordsPerSegment,
                                                .maxRecordBytes = options.maxRecordBytes,
                                                .closedCleanly = 0,
                                                .reserved = 0,
                                                .committedCount = 0};

    recordsPerSegment_ = options.recordsPerSegment;
    maxRecordBytes_ = options.maxRecordBytes;
    committedCount_ = 0;
    openSegment(0, /*create=*/true);
  }

  void openExisting(const std::filesystem::path& manifestPath) {
    manifestFile_ = MappedFile::openExisting(manifestPath, /*readOnly=*/false);
    if (manifestFile_.size() < sizeof(JournalManifest)) {
      throw JournalFormatError("manifest smaller than JournalManifest: " + manifestPath.string());
    }
    const JournalManifest& m = manifest();
    if (m.magic != kManifestMagic) {
      throw JournalFormatError("bad magic in " + manifestPath.string());
    }
    if (m.version != kManifestVersion) {
      throw JournalFormatError("unsupported manifest version in " + manifestPath.string());
    }

    // Geometry comes from the manifest, never from the caller's options:
    // every segment already on disk was laid out with these numbers, and
    // changing them would silently misaddress all of them.
    recordsPerSegment_ = m.recordsPerSegment;
    maxRecordBytes_ = m.maxRecordBytes;

    // §6.3 recovery: the committed count alone is authoritative. A
    // record written but never published (a crash between step 1 and
    // step 3) is simply absent from this count and will be silently
    // overwritten by the next append() — nothing to detect or repair.
    committedCount_ = m.committedCount.load(std::memory_order_acquire);

    // The active segment is the one the NEXT record goes into.
    openSegment(segmentFor(committedCount_ + 1), /*create=*/true);

    // Recover the write offset within that segment. A segment holding no
    // published records starts at zero; otherwise the last published
    // record in it gives the next free byte directly.
    const std::uint64_t slot = slotFor(committedCount_ + 1);
    if (slot == 0) {
      nextDataOffset_ = 0;
    } else {
      const IndexEntry& last = indexEntries()[slot - 1];
      nextDataOffset_ = last.byteOffset + last.entryLength;
    }
    publishedDataOffset_.store(nextDataOffset_, std::memory_order_relaxed);
  }

  JournalManifest& manifest() noexcept {
    return *reinterpret_cast<JournalManifest*>(manifestFile_.data());
  }
  const JournalManifest& manifest() const noexcept {
    return *reinterpret_cast<const JournalManifest*>(manifestFile_.data());
  }
  IndexEntry* indexEntries() noexcept {
    return reinterpret_cast<IndexEntry*>(indexFile_.data() + sizeof(IndexHeader));
  }

  // --- background segment work -------------------------------------
  //
  // One thread doing two jobs, both formerly inline in roll(): creating
  // and mapping the NEXT segment before it is needed, and flushing then
  // renaming the one just filled. The apply thread never blocks on
  // either in steady state.

  static constexpr std::uint64_t kNoSegment = ~0ULL;
  // How full the active segment must get before the next one is built.
  // Far enough ahead that creation and mapping finish first; late enough
  // that a short-lived journal never builds a segment it does not use.
  static constexpr std::uint64_t kPrepareAtPercent = 90;

  struct PendingSeal {
    MappedFile data;
    MappedFile index;
    std::uint64_t firstSequenceNumber;
    std::uint64_t lastSequenceNumber;
  };

  // Caller holds workMutex_.
  //
  // preparingSegment_ is checked as well as the other two, and that is
  // not redundant: the worker clears pendingPrepare_ when it PICKS UP a
  // job, so between then and the job finishing this would otherwise
  // queue the same segment a second time -- and the second
  // createSegmentFiles() would run MappedFile::createNew over the file
  // pair the writer had by then adopted and was appending into.
  void requestPrepareLocked(std::uint64_t segment) {
    if (preparedSegment_ == segment || pendingPrepare_ == segment ||
        preparingSegment_ == segment) {
      return;
    }
    pendingPrepare_ = segment;
    workCv_.notify_all();
  }

  void startWorker() {
    segmentWorker_ = std::thread([this] {
      for (;;) {
        std::uint64_t toPrepare = kNoSegment;
        PendingSeal seal;
        bool haveSeal = false;
        {
          std::unique_lock<std::mutex> lock(workMutex_);
          workCv_.wait(lock, [&] {
            return stopWorker_ || pendingPrepare_ != kNoSegment || !seals_.empty();
          });
          if (stopWorker_ && seals_.empty() && pendingPrepare_ == kNoSegment) {
            return;
          }
          if (pendingPrepare_ != kNoSegment) {
            toPrepare = pendingPrepare_;
            pendingPrepare_ = kNoSegment;
            preparingSegment_ = toPrepare;
          } else {
            seal = std::move(seals_.front());
            seals_.pop_front();
            haveSeal = true;
          }
        }

        if (toPrepare != kNoSegment) {
          MappedFile data, index;
          std::exception_ptr err;
          try {
            createSegmentFiles(toPrepare, data, index);
          } catch (...) {
            err = std::current_exception();
          }
          {
            std::lock_guard<std::mutex> lock(workMutex_);
            preparingSegment_ = kNoSegment;
            if (err != nullptr) {
              preparedError_ = err;
            } else {
              preparedData_ = std::move(data);
              preparedIndex_ = std::move(index);
              preparedSegment_ = toPrepare;
              preparedReady_ = true;
            }
          }
          workCv_.notify_all();
          continue;
        }

        if (haveSeal) {
          // Flush BEFORE rename: see roll().
          seal.data.flush(/*async=*/false);
          seal.index.flush(/*async=*/false);
          const std::string openStem = openSegmentStem(seal.firstSequenceNumber);
          const std::string sealedStem =
              sealedSegmentStem(seal.firstSequenceNumber, seal.lastSequenceNumber);
          for (const char* suffix : {".data", ".index"}) {
            std::error_code ec;
            std::filesystem::rename(segmentPath(openStem, suffix),
                                     segmentPath(sealedStem, suffix), ec);
            // A failed seal leaves the segment under its in-progress
            // name, which readers still resolve. Losing the rename is
            // not worth taking the node down for, and there is no
            // caller on this thread to throw to.
          }
          {
            std::lock_guard<std::mutex> lock(workMutex_);
            sealsInFlight_ -= 1;
          }
          workCv_.notify_all();
        }
      }
    });
  }

  // Blocks until every queued seal has been flushed and renamed.
  void drainSeals() {
    if (!segmentWorker_.joinable()) {
      return;
    }
    std::unique_lock<std::mutex> lock(workMutex_);
    workCv_.wait(lock, [&] { return seals_.empty() && sealsInFlight_ == 0; });
  }

  void stopWorker() {
    if (!segmentWorker_.joinable()) {
      return;
    }
    {
      std::lock_guard<std::mutex> lock(workMutex_);
      stopWorker_ = true;
    }
    workCv_.notify_all();
    segmentWorker_.join();
  }

  std::thread segmentWorker_;
  std::mutex workMutex_;
  std::condition_variable workCv_;
  bool stopWorker_ = false;
  std::uint64_t pendingPrepare_ = kNoSegment;
  std::uint64_t preparingSegment_ = kNoSegment;
  std::uint64_t preparedSegment_ = kNoSegment;
  MappedFile preparedData_;
  MappedFile preparedIndex_;
  bool preparedReady_ = false;
  std::exception_ptr preparedError_;
  std::deque<PendingSeal> seals_;
  int sealsInFlight_ = 0;

  std::filesystem::path dir_;
  MappedFile manifestFile_;
  MappedFile dataFile_;
  MappedFile indexFile_;
  std::uint64_t recordsPerSegment_ = 0;
  std::uint64_t maxRecordBytes_ = 0;
  std::uint64_t activeSegment_ = 0;
  std::uint64_t committedCount_ = 0;
  std::uint64_t nextDataOffset_ = 0;
  // See fillPercent(): a cross-thread-readable mirror of
  // nextDataOffset_, which is otherwise the apply thread's alone.
  std::atomic<std::uint64_t> publishedDataOffset_{0};
};

}  // namespace sequencer::journal
