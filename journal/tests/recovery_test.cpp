// Crash-recovery and defensive-open tests (specification.md §6.2, §6.3):
// a torn write (crash between step 1/2 and step 3) must be invisible
// and safely overwritten on the next append; a bad magic, bad version,
// truncated index, or inconsistent file pair must all fail loudly on
// open rather than silently misreading garbage.

#include <sequencer/journal/mapped_file.hpp>
#include <sequencer/journal/reader.hpp>
#include <sequencer/journal/record_view.hpp>
#include <sequencer/journal/writer.hpp>

#include <fstream>
#include <string>
#include <system_error>
#include <vector>

#include <gtest/gtest.h>

#include "test_temp_dir.hpp"

namespace sequencer::journal {
namespace {

Payload bytesOf(const std::string& s) {
  return Payload(reinterpret_cast<const std::byte*>(s.data()), s.size());
}

std::string toString(Payload p) {
  return std::string(reinterpret_cast<const char*>(p.data()), p.size());
}

// Replicates steps 1 and 2 of the write protocol (§6.3) directly,
// bypassing JournalWriter, and deliberately omits step 3 — simulating a
// crash after the record's bytes and index entry are durable but before
// the committed count advances.
void writeStepsOneAndTwoOnly(const std::filesystem::path& dataPath,
                              const std::filesystem::path& indexPath,
                              std::uint64_t sequenceNumber, Payload input,
                              std::span<const Payload> outputs) {
  MappedFile dataFile = MappedFile::openExisting(dataPath, /*readOnly=*/false);
  MappedFile indexFile = MappedFile::openExisting(indexPath, /*readOnly=*/false);

  auto& header = *reinterpret_cast<IndexHeader*>(indexFile.data());
  auto* entries = reinterpret_cast<IndexEntry*>(indexFile.data() + sizeof(IndexHeader));

  const std::uint64_t committed = header.committedCount.load(std::memory_order_acquire);
  std::uint64_t offset = 0;
  if (committed > 0) {
    offset = entries[committed - 1].byteOffset + entries[committed - 1].entryLength;
  }

  const std::size_t size = recordEncodedSize(input, outputs);
  encodeRecord(dataFile.data() + offset, sequenceNumber, input, outputs);  // step 1
  entries[committed] =
      IndexEntry{.byteOffset = offset, .entryLength = static_cast<std::uint32_t>(size), .reserved = 0};  // step 2
  // Deliberately no committedCount store here: step 3 never happens.

  dataFile.flush(/*async=*/false);
  indexFile.flush(/*async=*/false);
}

TEST(Recovery, TornWriteIsInvisibleAndSafelyOverwritten) {
  testing::TempDir dir;
  {
    JournalWriter writer(dir.dataPath(), dir.indexPath());
    writer.append(1, bytesOf("a"), {});
    writer.append(2, bytesOf("b"), {});
  }

  // Simulate a crash mid-append of record 3.
  writeStepsOneAndTwoOnly(dir.dataPath(), dir.indexPath(), 3, bytesOf("torn-and-should-vanish"), {});

  {
    JournalReader reader(dir.dataPath(), dir.indexPath());
    EXPECT_EQ(reader.committedCount(), 2u);
    EXPECT_THROW(reader.record(3), std::out_of_range);
  }

  // Recovery: reopening resumes at sequence 3, unaffected by the torn
  // write, and the next real append overwrites its bytes.
  JournalWriter writer(dir.dataPath(), dir.indexPath());
  EXPECT_EQ(writer.nextSequenceNumber(), 3u);
  writer.append(3, bytesOf("real-record-three"), {});
  writer.flush(false);

  JournalReader reader(dir.dataPath(), dir.indexPath());
  ASSERT_EQ(reader.committedCount(), 3u);
  EXPECT_EQ(toString(reader.record(3).input()), "real-record-three");
}

TEST(Recovery, BadMagicRefusesToOpen) {
  testing::TempDir dir;
  { JournalWriter writer(dir.dataPath(), dir.indexPath()); }

  {
    MappedFile indexFile = MappedFile::openExisting(dir.indexPath(), /*readOnly=*/false);
    auto& header = *reinterpret_cast<IndexHeader*>(indexFile.data());
    header.magic = 0xdeadbeef;
    indexFile.flush(false);
  }

  EXPECT_THROW(JournalReader(dir.dataPath(), dir.indexPath()), JournalFormatError);
  EXPECT_THROW(JournalWriter(dir.dataPath(), dir.indexPath()), JournalFormatError);
}

TEST(Recovery, BadVersionRefusesToOpen) {
  testing::TempDir dir;
  { JournalWriter writer(dir.dataPath(), dir.indexPath()); }

  {
    MappedFile indexFile = MappedFile::openExisting(dir.indexPath(), /*readOnly=*/false);
    auto& header = *reinterpret_cast<IndexHeader*>(indexFile.data());
    header.version = kIndexVersion + 1;
    indexFile.flush(false);
  }

  EXPECT_THROW(JournalReader(dir.dataPath(), dir.indexPath()), JournalFormatError);
  EXPECT_THROW(JournalWriter(dir.dataPath(), dir.indexPath()), JournalFormatError);
}

TEST(Recovery, TruncatedIndexFileRefusesToOpen) {
  testing::TempDir dir;
  { std::ofstream(dir.dataPath(), std::ios::binary) << "irrelevant"; }
  { std::ofstream(dir.indexPath(), std::ios::binary) << "short"; }  // < sizeof(IndexHeader)

  EXPECT_THROW(JournalReader(dir.dataPath(), dir.indexPath()), JournalFormatError);
  EXPECT_THROW(JournalWriter(dir.dataPath(), dir.indexPath()), JournalFormatError);
}

TEST(Recovery, InconsistentFilePairRefusesToOpen) {
  testing::TempDir dir;
  { std::ofstream(dir.dataPath(), std::ios::binary) << "data-only, no index"; }

  EXPECT_THROW(JournalWriter(dir.dataPath(), dir.indexPath()), JournalFormatError);
}

}  // namespace
}  // namespace sequencer::journal
