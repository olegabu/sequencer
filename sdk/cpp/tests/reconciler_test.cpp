#include <sequencer/sdk/reconciler.hpp>

#include <sequencer/temp_dir.hpp>
#include <sequencer/journal/writer.hpp>

#include <cstdint>
#include <filesystem>
#include <map>
#include <stdexcept>
#include <string>
#include <vector>

#include <gtest/gtest.h>

namespace sequencer::sdk {
namespace {


Payload payloadOf(const std::int64_t& v) {
  return Payload(reinterpret_cast<const std::byte*>(&v), sizeof(v));
}

Bytes rawRecordBytesFor(std::uint64_t sequenceNumber, std::int64_t input) {
  const std::size_t size = journal::recordEncodedSize(payloadOf(input), {});
  Bytes raw(size);
  journal::encodeRecord(raw.data(), sequenceNumber, payloadOf(input), {});
  return raw;
}

TEST(Reconciler, MatchingBytesReconcileCleanly) {
  const std::filesystem::path dir = sequencer::makeTempDir("reconciler_test");
  {
    journal::JournalWriter writer(dir / "journal");
    writer.append(1, payloadOf(5), {});
    writer.append(2, payloadOf(-2), {});
    writer.flush(false);
  }

  Reconciler reconciler(dir);
  const Bytes raw1 = rawRecordBytesFor(1, 5);
  EXPECT_FALSE(reconciler.check(1, Payload(raw1.data(), raw1.size())).has_value());
  const Bytes raw2 = rawRecordBytesFor(2, -2);
  EXPECT_FALSE(reconciler.check(2, Payload(raw2.data(), raw2.size())).has_value());

  std::filesystem::remove_all(dir);
}

TEST(Reconciler, MismatchedBytesAreReportedAsAFraudProof) {
  const std::filesystem::path dir = sequencer::makeTempDir("reconciler_test");
  {
    journal::JournalWriter writer(dir / "journal");
    writer.append(1, payloadOf(5), {});
    writer.flush(false);
  }

  Reconciler reconciler(dir);
  const Bytes wrongRaw = rawRecordBytesFor(1, 999);  // the client's own retained input was 5, not 999
  const std::optional<ReconciliationMismatch> mismatch =
      reconciler.check(1, Payload(wrongRaw.data(), wrongRaw.size()));
  ASSERT_TRUE(mismatch.has_value());
  EXPECT_EQ(mismatch->sequenceNumber, 1u);
  EXPECT_EQ(mismatch->kind, ReconciliationMismatch::Kind::kBytesDiffer);

  std::filesystem::remove_all(dir);
}

TEST(Reconciler, NotYetCommittedIsDistinguishedFromAMismatch) {
  const std::filesystem::path dir = sequencer::makeTempDir("reconciler_test");
  {
    journal::JournalWriter writer(dir / "journal");
    writer.append(1, payloadOf(5), {});
    writer.flush(false);
  }

  Reconciler reconciler(dir);
  const Bytes raw2 = rawRecordBytesFor(2, -2);  // never committed
  const std::optional<ReconciliationMismatch> mismatch = reconciler.check(2, Payload(raw2.data(), raw2.size()));
  ASSERT_TRUE(mismatch.has_value());
  EXPECT_EQ(mismatch->kind, ReconciliationMismatch::Kind::kNotYetCommitted);

  std::filesystem::remove_all(dir);
}

TEST(Reconciler, CheckAllReturnsOnlyTheMismatches) {
  const std::filesystem::path dir = sequencer::makeTempDir("reconciler_test");
  {
    journal::JournalWriter writer(dir / "journal");
    writer.append(1, payloadOf(5), {});
    writer.append(2, payloadOf(-2), {});
    writer.flush(false);
  }

  Reconciler reconciler(dir);
  std::map<std::uint64_t, Bytes> retained;
  retained[1] = rawRecordBytesFor(1, 5);      // matches
  retained[2] = rawRecordBytesFor(2, 12345);  // mismatch
  retained[3] = rawRecordBytesFor(3, 0);      // not yet committed

  const std::vector<ReconciliationMismatch> mismatches = reconciler.checkAll(retained);
  ASSERT_EQ(mismatches.size(), 2u);
  EXPECT_EQ(mismatches[0].sequenceNumber, 2u);
  EXPECT_EQ(mismatches[0].kind, ReconciliationMismatch::Kind::kBytesDiffer);
  EXPECT_EQ(mismatches[1].sequenceNumber, 3u);
  EXPECT_EQ(mismatches[1].kind, ReconciliationMismatch::Kind::kNotYetCommitted);

  std::filesystem::remove_all(dir);
}

}  // namespace
}  // namespace sequencer::sdk
