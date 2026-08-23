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

void tailAndVerify(const std::filesystem::path& dataPath, const std::filesystem::path& indexPath,
                    std::uint64_t count, bool pureSpin) {
  JournalReader reader(dataPath, indexPath);
  for (std::uint64_t seq = 1; seq <= count; ++seq) {
    RecordView r = reader.waitForRecord(seq, pureSpin);
    ASSERT_EQ(r.sequenceNumber(), seq);
    ASSERT_EQ(toString(r.input()), expectedInput(seq));
    ASSERT_EQ(r.outputCount(), 1u);
    ASSERT_EQ(toString(r.output(0)), expectedOutput(seq));
  }
}

TEST(Concurrency, ReadersNeverObserveATornRecordWhileWriterAppends) {
  testing::TempDir dir;
  constexpr std::uint64_t kCount = 20000;

  // Created up front, single-threaded: file creation itself is not the
  // protocol under test.
  JournalWriter writer(dir.dataPath(), dir.indexPath());

  std::thread readerA([&] { tailAndVerify(dir.dataPath(), dir.indexPath(), kCount, /*pureSpin=*/false); });
  std::thread readerB([&] { tailAndVerify(dir.dataPath(), dir.indexPath(), kCount, /*pureSpin=*/true); });

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
