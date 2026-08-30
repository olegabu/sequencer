// specification.md §14's acceptance checklist, item 4: "an output
// gateway restarted from an arbitrary sequence number produces
// dissemination identical to an uninterrupted run." output_gateway_test.cpp
// already proves a restarted gateway never redelivers what it already
// processed; this test proves the stronger, literal claim the
// checklist actually asks for — run gateway A uninterrupted end to end,
// run gateway B through a stop-and-restart at an arbitrary point, and
// assert the two delivered sequences are byte-for-byte identical.
//
// Shares collecting_stream_client.hpp's Subscription/subscribe/
// waitForCount with output_gateway_test.cpp — see that header's file
// comment for why this one, unlike other small test-local doubles in
// this repository, is worth not duplicating: an earlier duplicated
// copy of this same handler (in both this file and
// output_gateway_test.cpp) was almost certainly the cause of a rare,
// one-off segfault caught while this test was first stress-tested — a
// client-side counterpart to the server-side BrpcStreamFanout::closeAll()
// race documented elsewhere in this component's README.

#include <sequencer/brpc_output_transport.hpp>
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

class EchoOutputCodec : public sequencer::OutputCodec {
 public:
  void toOutput(const journal::RecordView& record, Fanout& fanout) override {
    const Payload input = record.input();
    fanout.broadcast("all", Bytes(input.begin(), input.end()));
  }
};

std::filesystem::path makeTempDir() {
  std::string tmpl = (std::filesystem::temp_directory_path() / "restart_drill_test_XXXXXX").string();
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

std::vector<std::int64_t> decode(const std::vector<std::string>& received) {
  std::vector<std::int64_t> values;
  values.reserve(received.size());
  for (const std::string& msg : received) {
    std::int64_t v;
    std::memcpy(&v, msg.data(), sizeof(v));
    values.push_back(v);
  }
  return values;
}

TEST(RestartDrill, RestartedGatewayProducesDisseminationIdenticalToAnUninterruptedRun) {
  const std::vector<std::int64_t> allValues = {5, -2, 10, -13, 100, 7, -8, 42, -1, 0, 99, -50};
  constexpr std::size_t kRestartAfter = 5;  // an arbitrary sequence number, not a block/round boundary

  // Gateway A: one continuous run, subscribed throughout — the
  // reference "uninterrupted run" this drill compares against.
  const std::filesystem::path dirA = makeTempDir();
  OutputGatewayConfig configA;
  configA.dataDir = dirA;
  configA.resumeFile = dirA / "resume";
  constexpr int portA = 28981;
  OutputGatewayImpl gatewayA(configA, std::make_unique<EchoOutputCodec>(), std::make_unique<BrpcOutputTransport>(), portA);
  gatewayA.start();

  brpc::Channel channelA;
  brpc::ChannelOptions channelOptionsA;
  channelOptionsA.timeout_ms = 2000;
  ASSERT_EQ(channelA.Init("127.0.0.1:28981", &channelOptionsA), 0);
  Subscription subA = subscribe(channelA, "all");
  appendRecords(dirA, 1, allValues);
  ASSERT_TRUE(waitForCount(subA.handler(), allValues.size(), std::chrono::seconds(5)));
  const std::vector<std::int64_t> deliveredByA = decode(subA.handler().snapshot());
  subA.close();  // done with gateway A's subscription well before gateway B's cycles begin
  gatewayA.stop();

  // Gateway B: stopped and restarted partway through, at an arbitrary
  // sequence number — the interrupted run.
  const std::filesystem::path dirB = makeTempDir();
  OutputGatewayConfig configB;
  configB.dataDir = dirB;
  configB.resumeFile = dirB / "resume";
  constexpr int portB = 28982;

  std::vector<std::int64_t> deliveredByB;
  {
    OutputGatewayImpl gatewayB(configB, std::make_unique<EchoOutputCodec>(), std::make_unique<BrpcOutputTransport>(), portB);
    gatewayB.start();
    brpc::Channel channel;
    brpc::ChannelOptions channelOptions;
    channelOptions.timeout_ms = 2000;
    ASSERT_EQ(channel.Init("127.0.0.1:28982", &channelOptions), 0);
    Subscription sub = subscribe(channel, "all");

    const std::vector<std::int64_t> firstSegment(allValues.begin(), allValues.begin() + kRestartAfter);
    appendRecords(dirB, 1, firstSegment);
    ASSERT_TRUE(waitForCount(sub.handler(), firstSegment.size(), std::chrono::seconds(5)));
    const std::vector<std::int64_t> got = decode(sub.handler().snapshot());
    deliveredByB.insert(deliveredByB.end(), got.begin(), got.end());
    sub.close();
    gatewayB.stop();
    // The durable resume position now holds kRestartAfter + 1 —
    // exactly the "arbitrary sequence number" this drill restarts
    // from.
  }
  {
    OutputGatewayImpl gatewayB(configB, std::make_unique<EchoOutputCodec>(), std::make_unique<BrpcOutputTransport>(), portB);
    gatewayB.start();
    brpc::Channel channel;
    brpc::ChannelOptions channelOptions;
    channelOptions.timeout_ms = 2000;
    ASSERT_EQ(channel.Init("127.0.0.1:28982", &channelOptions), 0);
    // Subscribed while the gateway has nothing new yet (resumed at
    // kRestartAfter + 1, and the journal still only has the first
    // segment) — the same race-free pattern
    // output_gateway_test.cpp's restart test uses — then the rest of
    // the journal is appended live.
    Subscription sub = subscribe(channel, "all");

    const std::vector<std::int64_t> secondSegment(allValues.begin() + kRestartAfter, allValues.end());
    appendRecords(dirB, kRestartAfter + 1, secondSegment);
    ASSERT_TRUE(waitForCount(sub.handler(), secondSegment.size(), std::chrono::seconds(5)));
    const std::vector<std::int64_t> got = decode(sub.handler().snapshot());
    deliveredByB.insert(deliveredByB.end(), got.begin(), got.end());
    sub.close();
    gatewayB.stop();
  }

  ASSERT_EQ(deliveredByA, allValues) << "sanity: the uninterrupted run must itself deliver everything, in order";
  EXPECT_EQ(deliveredByB, deliveredByA)
      << "specification.md §14 item 4: a restarted gateway's total dissemination must be identical "
      << "to an uninterrupted run's, content and order alike";

  std::filesystem::remove_all(dirA);
  std::filesystem::remove_all(dirB);
}

}  // namespace
}  // namespace sequencer::gateway::output::detail
