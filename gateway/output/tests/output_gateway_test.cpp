// End-to-end test of the output gateway chassis (specification.md
// §8.3, §8.5): a real OutputGatewayImpl tailing a directly-synthesized
// journal (no node needed — journal/'s own writer is enough, matching
// examples/counter/tests/replay_test.cpp's pattern), delivered to a
// real client over a real brpc::Stream — including resume-after-
// restart, which is the whole point of the durable resume position
// (§8.3: "restartable from any sequence number with identical output").

#include "brpc_stream_transport.hpp"
#include "collecting_stream_client.hpp"
#include "output_gateway_impl.hpp"

#include <sequencer/journal/writer.hpp>

#include <brpc/channel.h>
#include <brpc/controller.h>

#include <chrono>
#include <cstring>
#include <filesystem>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

namespace sequencer::gateway::output::detail {
namespace {

// Broadcasts each record's raw input bytes, verbatim, to the "all"
// topic — enough to observe delivery order and content without any
// application-specific interpretation.
class EchoOutputCodec : public sequencer::OutputCodec {
 public:
  void toOutput(const journal::RecordView& record, Fanout& fanout) override {
    const Payload input = record.input();
    fanout.broadcast("all", Bytes(input.begin(), input.end()));
  }
};

std::filesystem::path makeTempDir() {
  std::string tmpl = (std::filesystem::temp_directory_path() / "output_gateway_test_XXXXXX").string();
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
  journal::JournalWriter writer(dataDir / "journal.data", dataDir / "journal.index");
  for (std::size_t i = 0; i < values.size(); ++i) {
    writer.append(startSeq + i, payloadOf(values[i]), {});
  }
  writer.flush(false);
}

TEST(OutputGateway, DeliversLiveRecordsInOrderToAConnectedSubscriber) {
  const std::filesystem::path dir = makeTempDir();

  OutputGatewayConfig config;
  config.dataDir = dir;
  config.resumeFile = dir / "resume";
  config.listenPort = 28961;
  OutputGatewayImpl gateway(config, std::make_unique<EchoOutputCodec>(),
                            std::make_unique<BrpcStreamTransport>());
  gateway.start();

  brpc::Channel channel;
  brpc::ChannelOptions channelOptions;
  channelOptions.timeout_ms = 2000;
  ASSERT_EQ(channel.Init("127.0.0.1:28961", &channelOptions), 0);

  // Subscribe before any record exists: Fanout delivers live to
  // currently-connected sessions only (no historical replay for late
  // joiners in this phase — see StreamFanout's header comment), so a
  // realistic test — and a real deployment — connects first.
  Subscription sub = subscribe(channel, "all");
  appendRecords(dir, 1, {5, -2, 10, -13, 100});
  ASSERT_TRUE(waitForCount(sub.handler(), 5, std::chrono::seconds(5)));

  const std::vector<std::string> received = sub.handler().snapshot();
  const std::vector<std::int64_t> expected = {5, -2, 10, -13, 100};
  ASSERT_EQ(received.size(), expected.size());
  for (std::size_t i = 0; i < expected.size(); ++i) {
    ASSERT_EQ(received[i].size(), sizeof(std::int64_t));
    std::int64_t got;
    std::memcpy(&got, received[i].data(), sizeof(got));
    EXPECT_EQ(got, expected[i]) << "record index " << i;
  }

  gateway.stop();
  std::filesystem::remove_all(dir);
}

TEST(OutputGateway, ResumesFromDurablePositionAfterRestartWithoutRedelivering) {
  const std::filesystem::path dir = makeTempDir();

  OutputGatewayConfig config;
  config.dataDir = dir;
  config.resumeFile = dir / "resume";
  config.listenPort = 28962;

  {
    OutputGatewayImpl gateway(config, std::make_unique<EchoOutputCodec>(),
                            std::make_unique<BrpcStreamTransport>());
    gateway.start();

    brpc::Channel channel;
    brpc::ChannelOptions channelOptions;
    channelOptions.timeout_ms = 2000;
    ASSERT_EQ(channel.Init("127.0.0.1:28962", &channelOptions), 0);

    Subscription sub = subscribe(channel, "all");
    appendRecords(dir, 1, {1, 2, 3});
    ASSERT_TRUE(waitForCount(sub.handler(), 3, std::chrono::seconds(5)));
    gateway.stop();
    // The resume file now durably holds 4 — the tailing loop advances
    // it as it processes records, independent of whether any
    // subscriber was connected to receive them.
  }

  {
    OutputGatewayImpl gateway(config, std::make_unique<EchoOutputCodec>(),
                            std::make_unique<BrpcStreamTransport>());
    gateway.start();

    brpc::Channel channel;
    brpc::ChannelOptions channelOptions;
    channelOptions.timeout_ms = 2000;
    ASSERT_EQ(channel.Init("127.0.0.1:28962", &channelOptions), 0);

    // Subscribe while the gateway has nothing new to deliver yet (it
    // resumed at 4; the journal still only has 1-3), then append the
    // records the resume position was durably tracking toward — this
    // is what actually proves the durable position survived the
    // restart: the subscriber sees only 40 and 50, never 1-3 again.
    Subscription sub = subscribe(channel, "all");
    appendRecords(dir, 4, {40, 50});
    ASSERT_TRUE(waitForCount(sub.handler(), 2, std::chrono::seconds(5)));

    const std::vector<std::string> received = sub.handler().snapshot();
    ASSERT_EQ(received.size(), 2u);
    std::int64_t first, second;
    std::memcpy(&first, received[0].data(), sizeof(first));
    std::memcpy(&second, received[1].data(), sizeof(second));
    EXPECT_EQ(first, 40);
    EXPECT_EQ(second, 50);

    gateway.stop();
  }

  std::filesystem::remove_all(dir);
}

}  // namespace
}  // namespace sequencer::gateway::output::detail
