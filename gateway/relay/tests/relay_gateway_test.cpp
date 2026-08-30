// End-to-end test of the relay gateway (specification.md §8.2): a real
// RelayGatewayImpl tailing a directly-synthesized journal (no node
// needed — matching gateway/output/tests/output_gateway_test.cpp's
// pattern), served over a real brpc client — this repository's own
// reference RelaySubscribeClient — via RelayService.

#include "relay_gateway_impl.hpp"
#include "relay_server.hpp"

#include <sequencer/journal/writer.hpp>
#include <sequencer/relay/subscribe_client.hpp>

#include <algorithm>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

namespace sequencer::gateway::relay::detail {
namespace {

std::filesystem::path makeTempDir() {
  std::string tmpl = (std::filesystem::temp_directory_path() / "relay_gateway_test_XXXXXX").string();
  if (::mkdtemp(tmpl.data()) == nullptr) {
    throw std::runtime_error("mkdtemp failed");
  }
  return tmpl;
}

Payload payloadOf(const std::int64_t& v) {
  return Payload(reinterpret_cast<const std::byte*>(&v), sizeof(v));
}

void appendRecords(const std::filesystem::path& dataDir, std::uint64_t startSeq,
                    const std::vector<std::int64_t>& values) {
  journal::JournalWriter writer(dataDir / "journal");
  for (std::size_t i = 0; i < values.size(); ++i) {
    writer.append(startSeq + i, payloadOf(values[i]), {});
  }
  writer.flush(false);
}

// Collects every record a RelaySubscribeClient delivers, decoded back
// to the int64 values this test's records encode.
class CollectingClient {
 public:
  CollectingClient(const std::string& endpoint, std::uint64_t fromSequenceNumber)
      : client_(endpoint, fromSequenceNumber, [this](Bytes rawRecordBytes) { onRecord(std::move(rawRecordBytes)); }) {}

  bool ok() const { return client_.ok(); }
  const std::string& errorMessage() const { return client_.errorMessage(); }

  std::vector<std::int64_t> snapshot() {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<std::int64_t> values;
    values.reserve(rawRecords_.size());
    for (const Bytes& raw : rawRecords_) {
      const journal::RecordView view(raw.data(), static_cast<std::uint32_t>(raw.size()));
      std::int64_t v;
      std::memcpy(&v, view.input().data(), sizeof(v));
      values.push_back(v);
    }
    return values;
  }

  // The complete raw bytes of every record received so far, for a
  // byte-identical comparison against a colocated JournalReader.
  std::vector<Bytes> rawSnapshot() {
    std::lock_guard<std::mutex> lock(mutex_);
    return rawRecords_;
  }

 private:
  void onRecord(Bytes rawRecordBytes) {
    std::lock_guard<std::mutex> lock(mutex_);
    rawRecords_.push_back(std::move(rawRecordBytes));
  }

  std::mutex mutex_;
  std::vector<Bytes> rawRecords_;
  sequencer::relay::RelaySubscribeClient client_;
};

bool waitForCount(CollectingClient& client, std::size_t count, std::chrono::seconds timeout) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    if (client.snapshot().size() >= count) {
      return true;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  return false;
}

TEST(RelayGateway, SubscribingFromTheBeginningReplaysAlreadyCommittedHistory) {
  const std::filesystem::path dir = makeTempDir();
  appendRecords(dir, 1, {5, -2, 10, -13, 100});

  RelayGatewayConfig config;
  config.dataDir = dir;
  config.listenPort = 29051;
  RelayGatewayImpl gateway(std::move(config));
  gateway.start();
  RelayServer server(gateway, 29051);

  // No "subscribe before appending" dance needed here, unlike
  // gateway/output's Fanout — specification.md §8.2's whole point is
  // that a relay serves already-committed history exactly the same
  // way it serves live records.
  CollectingClient client("127.0.0.1:29051", /*fromSequenceNumber=*/0);
  ASSERT_TRUE(client.ok()) << client.errorMessage();
  ASSERT_TRUE(waitForCount(client, 5, std::chrono::seconds(5)));

  const std::vector<std::int64_t> expected = {5, -2, 10, -13, 100};
  EXPECT_EQ(client.snapshot(), expected);

  std::filesystem::remove_all(dir);
}

TEST(RelayGateway, DeliversLiveRecordsAppendedAfterSubscribing) {
  const std::filesystem::path dir = makeTempDir();
  appendRecords(dir, 1, {1, 2});  // some pre-existing history

  RelayGatewayConfig config;
  config.dataDir = dir;
  config.listenPort = 29052;
  RelayGatewayImpl gateway(std::move(config));
  gateway.start();
  RelayServer server(gateway, 29052);

  CollectingClient client("127.0.0.1:29052", /*fromSequenceNumber=*/0);
  ASSERT_TRUE(client.ok()) << client.errorMessage();
  ASSERT_TRUE(waitForCount(client, 2, std::chrono::seconds(5)));

  appendRecords(dir, 3, {30, 40});
  ASSERT_TRUE(waitForCount(client, 4, std::chrono::seconds(5)));

  const std::vector<std::int64_t> expected = {1, 2, 30, 40};
  EXPECT_EQ(client.snapshot(), expected);

  std::filesystem::remove_all(dir);
}

TEST(RelayGateway, EachSubscriberHasAnIndependentCursorFromItsOwnRequestedSequenceNumber) {
  const std::filesystem::path dir = makeTempDir();
  appendRecords(dir, 1, {10, 20, 30, 40, 50});

  RelayGatewayConfig config;
  config.dataDir = dir;
  config.listenPort = 29053;
  RelayGatewayImpl gateway(std::move(config));
  gateway.start();
  RelayServer server(gateway, 29053);

  CollectingClient fromStart("127.0.0.1:29053", /*fromSequenceNumber=*/0);
  CollectingClient fromMiddle("127.0.0.1:29053", /*fromSequenceNumber=*/3);
  ASSERT_TRUE(fromStart.ok()) << fromStart.errorMessage();
  ASSERT_TRUE(fromMiddle.ok()) << fromMiddle.errorMessage();

  ASSERT_TRUE(waitForCount(fromStart, 5, std::chrono::seconds(5)));
  ASSERT_TRUE(waitForCount(fromMiddle, 3, std::chrono::seconds(5)));

  EXPECT_EQ(fromStart.snapshot(), (std::vector<std::int64_t>{10, 20, 30, 40, 50}));
  EXPECT_EQ(fromMiddle.snapshot(), (std::vector<std::int64_t>{30, 40, 50}))
      << "specification.md §8.2: any number of concurrent subscribers, each independently";

  EXPECT_EQ(gateway.sessionCount(), 2u);

  std::filesystem::remove_all(dir);
}

TEST(RelayGateway, DeliveredRecordsAreByteIdenticalToTheColocatedJournal) {
  const std::filesystem::path dir = makeTempDir();
  appendRecords(dir, 1, {7, -8, 42});

  RelayGatewayConfig config;
  config.dataDir = dir;
  config.listenPort = 29054;
  RelayGatewayImpl gateway(std::move(config));
  gateway.start();
  RelayServer server(gateway, 29054);

  CollectingClient client("127.0.0.1:29054", /*fromSequenceNumber=*/0);
  ASSERT_TRUE(client.ok()) << client.errorMessage();
  ASSERT_TRUE(waitForCount(client, 3, std::chrono::seconds(5)));

  journal::JournalReader reader(dir / "journal");
  const std::vector<Bytes> received = client.rawSnapshot();
  ASSERT_EQ(received.size(), 3u);
  for (std::uint64_t seq = 1; seq <= 3; ++seq) {
    const Payload expected = reader.record(seq).rawBytes();
    const Bytes& got = received[seq - 1];
    ASSERT_EQ(got.size(), expected.size()) << "seq " << seq;
    EXPECT_TRUE(std::equal(got.begin(), got.end(), expected.begin()))
        << "seq " << seq << ": specification.md §8.2's \"byte-identical records\"";
  }

  std::filesystem::remove_all(dir);
}

}  // namespace
}  // namespace sequencer::gateway::relay::detail
