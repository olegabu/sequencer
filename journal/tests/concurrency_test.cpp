// Cross-thread acquire/release protocol test (specification.md §6.3): a
// writer thread appends while reader threads tail concurrently, each
// through its own independently-opened mapping (mimicking separate
// processes). No reader may ever observe a torn or partial record. This
// is the test the tsan CMake preset exists for — the memory-model claim
// in §6.3 is checked here, not assumed.

#include <sequencer/journal/reader.hpp>
#include <sequencer/journal/writer.hpp>

#include <string>
#include <thread>
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

std::string expectedInput(std::uint64_t seq) { return "record-" + std::to_string(seq); }
std::string expectedOutput(std::uint64_t seq) { return "output-" + std::to_string(seq); }

void tailAndVerify(const std::filesystem::path& dir, std::uint64_t count, bool pureSpin) {
  JournalReader reader(dir);
  for (std::uint64_t seq = 1; seq <= count; ++seq) {
    RecordView r = reader.waitForRecord(seq, pureSpin);
    ASSERT_EQ(r.sequenceNumber(), seq);
    ASSERT_EQ(toString(r.input()), expectedInput(seq));
    ASSERT_EQ(r.outputCount(), 1u);
    ASSERT_EQ(toString(r.output(0)), expectedOutput(seq));
  }
}

// The rollover race (§6.5), which the test below does NOT reach because
// it uses the default geometry and never crosses a segment boundary.
//
// Two things are racing here that do not race in a single-segment
// journal: the writer seals a segment by renaming it while readers may
// be opening it by name, and several reader threads may simultaneously
// discover the same brand-new segment and try to map it. The first is
// why openSegment() tries the sealed name, then the open one, and
// retries; the second is why the segment cache installs with a CAS and
// the loser drops its own mapping. Both are the kind of thing that
// works in casual testing and fails under load, so this is the test to
// run under ThreadSanitizer.
TEST(Concurrency, ReadersFollowTheWriterAcrossSegmentBoundaries) {
  testing::TempDir dir;
  constexpr std::uint64_t kCount = 5000;

  JournalOptions options;
  options.recordsPerSegment = 64;  // ~78 rolls over the run
  options.maxRecordBytes = 256;
  JournalWriter writer(dir.path(), options);

  // Both readers are constructed while only segment zero exists, so
  // every later segment is discovered from the committed count alone.
  std::thread readerA([&] { tailAndVerify(dir.path(), kCount, /*pureSpin=*/false); });
  std::thread readerB([&] { tailAndVerify(dir.path(), kCount, /*pureSpin=*/true); });

  for (std::uint64_t seq = 1; seq <= kCount; ++seq) {
    const std::string in = expectedInput(seq);
    const std::string out = expectedOutput(seq);
    std::vector<Payload> outputs = {bytesOf(out)};
    writer.append(seq, bytesOf(in), outputs);
  }

  readerA.join();
  readerB.join();
  EXPECT_GT(writer.segmentCount(), 1u) << "this test is pointless if it never rolled";
}

TEST(Concurrency, ReadersNeverObserveATornRecordWhileWriterAppends) {
  testing::TempDir dir;
  constexpr std::uint64_t kCount = 20000;

  // Created up front, single-threaded: file creation itself is not the
  // protocol under test.
  JournalWriter writer(dir.path());

  std::thread readerA([&] { tailAndVerify(dir.path(), kCount, /*pureSpin=*/false); });
  std::thread readerB([&] { tailAndVerify(dir.path(), kCount, /*pureSpin=*/true); });

  for (std::uint64_t seq = 1; seq <= kCount; ++seq) {
    const std::string in = expectedInput(seq);
    const std::string out = expectedOutput(seq);
    std::vector<Payload> outputs = {bytesOf(out)};
    writer.append(seq, bytesOf(in), outputs);
  }

  readerA.join();
  readerB.join();
}

}  // namespace
}  // namespace sequencer::journal
