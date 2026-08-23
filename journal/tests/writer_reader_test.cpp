// Writer/reader round-trip tests over real files (specification.md §6):
// single- and multi-record round trips, O(1) random access, reopen-and-
// continue, and the dense-sequence defense in append().

#include <sequencer/journal/reader.hpp>
#include <sequencer/journal/writer.hpp>

#include <algorithm>
#include <string>
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

TEST(WriterReader, SingleRecordRoundTripVaryingOutputCounts) {
  testing::TempDir dir;
  JournalWriter writer(dir.dataPath(), dir.indexPath());

  writer.append(1, bytesOf("in-zero-outputs"), {});

  // Named locals, not bytesOf("literal") inline in the vector: a
  // temporary std::string bound to bytesOf's parameter is destroyed at
  // the end of that statement, so a Payload captured from it and read
  // on a later statement (append(), below) would dangle.
  const std::string onlyStr = "only";
  std::vector<Payload> oneOutput = {bytesOf(onlyStr)};
  writer.append(2, bytesOf("in-one-output"), oneOutput);

  const std::string o0 = "o0";
  const std::string o1 = "o1";
  const std::string o2 = "o2";
  std::vector<Payload> threeOutputs = {bytesOf(o0), bytesOf(o1), bytesOf(o2)};
  writer.append(3, bytesOf("in-three-outputs"), threeOutputs);

  writer.flush(/*async=*/false);

  JournalReader reader(dir.dataPath(), dir.indexPath());
  ASSERT_EQ(reader.committedCount(), 3u);

  {
    RecordView r = reader.record(1);
    EXPECT_EQ(r.sequenceNumber(), 1u);
    EXPECT_EQ(toString(r.input()), "in-zero-outputs");
    EXPECT_EQ(r.outputCount(), 0u);
  }
  {
    RecordView r = reader.record(2);
    EXPECT_EQ(toString(r.input()), "in-one-output");
    ASSERT_EQ(r.outputCount(), 1u);
    EXPECT_EQ(toString(r.output(0)), "only");
  }
  {
    RecordView r = reader.record(3);
    EXPECT_EQ(toString(r.input()), "in-three-outputs");
    ASSERT_EQ(r.outputCount(), 3u);
    EXPECT_EQ(toString(r.output(0)), "o0");
    EXPECT_EQ(toString(r.output(1)), "o1");
    EXPECT_EQ(toString(r.output(2)), "o2");
  }
}

TEST(WriterReader, ManyRecordsSupportO1RandomAccess) {
  testing::TempDir dir;
  JournalWriter writer(dir.dataPath(), dir.indexPath());

  constexpr int kCount = 500;
  for (int i = 1; i <= kCount; ++i) {
    const std::string in = "input-" + std::to_string(i);
    const std::string out = "output-" + std::to_string(i);
    std::vector<Payload> outputs = {bytesOf(out)};
    writer.append(static_cast<std::uint64_t>(i), bytesOf(in), outputs);
  }
  writer.flush(false);

  JournalReader reader(dir.dataPath(), dir.indexPath());
  ASSERT_EQ(reader.committedCount(), static_cast<std::uint64_t>(kCount));

  // Access out of monotonic order to exercise real random access, not
  // just sequential scanning.
  for (int i : {1, kCount, kCount / 2, 2, kCount - 1}) {
    RecordView r = reader.record(static_cast<std::uint64_t>(i));
    EXPECT_EQ(r.sequenceNumber(), static_cast<std::uint64_t>(i));
    EXPECT_EQ(toString(r.input()), "input-" + std::to_string(i));
    ASSERT_EQ(r.outputCount(), 1u);
    EXPECT_EQ(toString(r.output(0)), "output-" + std::to_string(i));
  }
}

TEST(WriterReader, LargeInputRoundTrips) {
  testing::TempDir dir;
  JournalWriter writer(dir.dataPath(), dir.indexPath());

  std::string big(2 * 1024 * 1024, 'x');
  for (std::size_t i = 0; i < big.size(); ++i) {
    big[i] = static_cast<char>('a' + (i % 26));
  }
  writer.append(1, bytesOf(big), {});
  writer.flush(false);

  JournalReader reader(dir.dataPath(), dir.indexPath());
  RecordView r = reader.record(1);
  EXPECT_EQ(toString(r.input()), big);
}

TEST(WriterReader, ReopenResumesAtNextSequenceNumberAndStaysDense) {
  testing::TempDir dir;
  {
    JournalWriter writer(dir.dataPath(), dir.indexPath());
    writer.append(1, bytesOf("a"), {});
    writer.append(2, bytesOf("b"), {});
    EXPECT_EQ(writer.nextSequenceNumber(), 3u);
  }  // writer destroyed: closedCleanly set, files flushed

  {
    JournalWriter writer(dir.dataPath(), dir.indexPath());
    EXPECT_EQ(writer.nextSequenceNumber(), 3u);
    writer.append(3, bytesOf("c"), {});
    writer.flush(false);
  }

  JournalReader reader(dir.dataPath(), dir.indexPath());
  ASSERT_EQ(reader.committedCount(), 3u);
  EXPECT_EQ(toString(reader.record(1).input()), "a");
  EXPECT_EQ(toString(reader.record(2).input()), "b");
  EXPECT_EQ(toString(reader.record(3).input()), "c");
}

TEST(WriterReader, AppendRejectsNonDenseSequenceNumber) {
  testing::TempDir dir;
  JournalWriter writer(dir.dataPath(), dir.indexPath());
  writer.append(1, bytesOf("a"), {});
  EXPECT_THROW(writer.append(3, bytesOf("skip-two"), {}), std::logic_error);
  EXPECT_THROW(writer.append(1, bytesOf("repeat"), {}), std::logic_error);
}

TEST(WriterReader, ReaderObservesAppendsThroughLiveMappingWithoutReopening) {
  testing::TempDir dir;
  JournalWriter writer(dir.dataPath(), dir.indexPath());
  writer.append(1, bytesOf("a"), {});
  writer.flush(false);

  JournalReader reader(dir.dataPath(), dir.indexPath());
  ASSERT_EQ(reader.committedCount(), 1u);

  writer.append(2, bytesOf("b"), {});
  writer.flush(false);

  // Same reader instance, no reopen: MAP_SHARED means both mappings
  // back the same pages, so the acquire-load simply sees the new value.
  EXPECT_EQ(reader.committedCount(), 2u);
  EXPECT_EQ(toString(reader.record(2).input()), "b");
}

TEST(WriterReader, RecordOutOfRangeThrows) {
  testing::TempDir dir;
  JournalWriter writer(dir.dataPath(), dir.indexPath());
  writer.append(1, bytesOf("a"), {});
  writer.flush(false);

  JournalReader reader(dir.dataPath(), dir.indexPath());
  EXPECT_THROW(reader.record(0), std::out_of_range);
  EXPECT_THROW(reader.record(2), std::out_of_range);
}

}  // namespace
}  // namespace sequencer::journal
