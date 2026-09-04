// End-to-end test of the output gateway chassis (specification.md
// §8.3, §8.5): a real OutputGatewayImpl tailing a directly-synthesized
// journal (no node needed — journal/'s own writer is enough, matching
// examples/counter/tests/replay_test.cpp's pattern), delivered to a
// real client over a real brpc::Stream — including resume-after-
// restart, which is the whole point of the durable resume position
// (§8.3: "restartable from any sequence number with identical output").

#include <sequencer/temp_dir.hpp>
#include <sequencer/brpc_output_transport.hpp>
#include "collecting_stream_client.hpp"
#include "output_gateway_impl.hpp"

#include <sequencer/websocket_output_transport.hpp>

#include <boost/asio/connect.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/websocket.hpp>

#include <cstdint>
#include <deque>
#include <memory>
#include <stdexcept>

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

TEST(OutputGateway, DeliversLiveRecordsInOrderToAConnectedSubscriber) {
  const std::filesystem::path dir = sequencer::makeTempDir("output_gateway_test");

  OutputGatewayConfig config;
  config.dataDir = dir;
  config.resumeFile = dir / "resume";
  OutputGatewayImpl gateway(config, std::make_unique<EchoOutputCodec>(),
                            std::make_unique<BrpcOutputTransport>(), 28961);
  gateway.start();

  brpc::Channel channel;
  brpc::ChannelOptions channelOptions;
  channelOptions.timeout_ms = 2000;
  ASSERT_EQ(channel.Init("127.0.0.1:28961", &channelOptions), 0);

  // Subscribe before any record exists: Fanout delivers live to
  // currently-connected sessions only (no historical replay for late
  // joiners in this phase — see BrpcStreamFanout's header comment), so a
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
  const std::filesystem::path dir = sequencer::makeTempDir("output_gateway_test");

  OutputGatewayConfig config;
  config.dataDir = dir;
  config.resumeFile = dir / "resume";
  {
    OutputGatewayImpl gateway(config, std::make_unique<EchoOutputCodec>(),
                            std::make_unique<BrpcOutputTransport>(), 28962);
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
                            std::make_unique<BrpcOutputTransport>(), 28962);
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

// A minimal Beast client, same shape as the one
// websocket_output_transport_test.cpp uses (including the
// length-prefix decode every WebSocket subscriber needs — see
// src/websocket_output_transport.cpp's own top comment). Local to this
// file rather than shared: it exists only for the two-transport case
// below.
class TestWsClient {
 public:
  TestWsClient(int port, const std::string& topic) : ws_(ioContext_) {
    namespace net = boost::asio;
    using tcp = net::ip::tcp;
    tcp::resolver resolver(ioContext_);
    const auto results = resolver.resolve("127.0.0.1", std::to_string(port));
    net::connect(ws_.next_layer(), results);
    ws_.handshake("127.0.0.1", "/" + topic);
  }

  std::string readOne() {
    if (buffered_.empty()) {
      boost::beast::flat_buffer buffer;
      ws_.read(buffer);
      const std::string frame = boost::beast::buffers_to_string(buffer.data());
      std::size_t offset = 0;
      while (offset + 4 <= frame.size()) {
        const auto length = static_cast<std::uint32_t>(static_cast<unsigned char>(frame[offset]) << 24 |
                                                          static_cast<unsigned char>(frame[offset + 1]) << 16 |
                                                          static_cast<unsigned char>(frame[offset + 2]) << 8 |
                                                          static_cast<unsigned char>(frame[offset + 3]));
        offset += 4;
        if (offset + length > frame.size()) break;
        buffered_.push_back(frame.substr(offset, length));
        offset += length;
      }
      if (buffered_.empty()) {
        throw std::runtime_error("server sent a frame with no decodable payloads");
      }
    }
    std::string payload = std::move(buffered_.front());
    buffered_.pop_front();
    return payload;
  }

 private:
  boost::asio::io_context ioContext_;
  boost::beast::websocket::stream<boost::asio::ip::tcp::socket> ws_;
  std::deque<std::string> buffered_;
};

// The property the multi-transport chassis exists for: one journal
// tail, one codec pass, one ring — and every attached transport's
// subscribers see every record. Two different protocols on two ports,
// served by a single gateway, both receiving the identical sequence.
//
// EchoOutputCodec's payloads are raw 8-byte integers, which is
// load-bearing here rather than incidental: they are not valid UTF-8,
// and this test is what caught the WebSocket transport sending text
// frames (RFC 6455 requires text payloads be UTF-8, so Beast refused
// the write and the fanout dropped it silently). The brpc subscriber
// received every record while the WebSocket one received none. See
// src/websocket_output_transport.cpp's wire-format comment.
TEST(OutputGateway, OneGatewayServesTwoTransportsFromOneJournalTail) {
  const std::filesystem::path dir = sequencer::makeTempDir("output_gateway_test");
  constexpr int kBrpcPort = 28963;
  constexpr int kWsPort = 28964;

  OutputGatewayConfig config;
  config.dataDir = dir;
  config.resumeFile = dir / "resume";

  std::vector<OutputGatewayImpl::Binding> bindings;
  bindings.push_back({std::make_unique<BrpcOutputTransport>(), kBrpcPort});
  bindings.push_back({std::make_unique<sequencer::WebSocketOutputTransport>(), kWsPort});
  OutputGatewayImpl gateway(config, std::make_unique<EchoOutputCodec>(), std::move(bindings));
  gateway.start();

  brpc::Channel channel;
  brpc::ChannelOptions channelOptions;
  channelOptions.timeout_ms = 2000;
  ASSERT_EQ(channel.Init("127.0.0.1:28963", &channelOptions), 0);
  Subscription brpcSub = subscribe(channel, "all");

  std::unique_ptr<TestWsClient> wsClient;
  for (int attempt = 0; attempt < 100 && !wsClient; ++attempt) {
    try {
      wsClient = std::make_unique<TestWsClient>(kWsPort, "all");
    } catch (const std::exception&) {
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
  }
  ASSERT_NE(wsClient, nullptr);
  // Both subscribers must be registered before anything is appended:
  // delivery is live-only for either transport.
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  const std::vector<std::int64_t> expected = {7, -3, 11, 42};
  appendRecords(dir, 1, expected);

  ASSERT_TRUE(waitForCount(brpcSub.handler(), expected.size(), std::chrono::seconds(5)));
  const std::vector<std::string> viaBrpc = brpcSub.handler().snapshot();
  ASSERT_EQ(viaBrpc.size(), expected.size());
  for (std::size_t i = 0; i < expected.size(); ++i) {
    std::int64_t got;
    std::memcpy(&got, viaBrpc[i].data(), sizeof(got));
    EXPECT_EQ(got, expected[i]) << "brpc subscriber, record " << i;

    const std::string wsMsg = wsClient->readOne();
    ASSERT_EQ(wsMsg.size(), sizeof(std::int64_t));
    std::int64_t wsGot;
    std::memcpy(&wsGot, wsMsg.data(), sizeof(wsGot));
    EXPECT_EQ(wsGot, expected[i]) << "websocket subscriber, record " << i;
  }

  gateway.stop();
  std::filesystem::remove_all(dir);
}

}  // namespace
}  // namespace sequencer::gateway::output::detail
