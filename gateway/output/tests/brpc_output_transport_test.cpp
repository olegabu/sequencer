// Tests for BrpcOutputTransport in isolation — a real brpc::Stream
// client connects over a real socket, and the test publishes tagged
// entries into a BroadcastRing the transport is attached to (exactly
// how the chassis's RingFanout does it), verifying actual network
// delivery through the per-subscriber reader path (not just that the
// code compiles against brpc's API). Mirrors
// grpc_output_transport_test.cpp and websocket_output_transport_test.cpp
// case for case.
//
// Until this file existed, brpc was the only one of the three
// transports without a test of its own: it was covered incidentally by
// output_gateway_test.cpp, which exercises the CHASSIS and happens to
// use brpc as its transport. That left the three flavors asymmetric in
// testing as well as in naming, and it meant a brpc-specific regression
// would surface as a chassis failure rather than as a transport one.

#include <sequencer/brpc_output_transport.hpp>

#include <brpc/channel.h>

#include <sequencer/broadcast_ring.hpp>

#include <chrono>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "collecting_stream_client.hpp"

namespace sequencer {
namespace {

using gateway::output::detail::subscribe;
using gateway::output::detail::Subscription;
using gateway::output::detail::waitForCount;

// Publishes exactly the way OutputGatewayImpl's RingFanout does —
// tagged with the interned topic id; readers filter on it.
void publishBroadcast(BroadcastRing& ring, TopicRegistry& topics, const std::string& topic,
                      const std::string& payload) {
  ring.publish(makeTopicTag(topics.idFor(topic)), reinterpret_cast<const std::byte*>(payload.data()),
               payload.size());
}

// brpc::Channel is neither copyable nor movable, so this initializes
// one in place rather than returning it.
void connect(brpc::Channel& channel, int port) {
  brpc::ChannelOptions options;
  options.protocol = brpc::PROTOCOL_BAIDU_STD;
  options.timeout_ms = 2000;
  if (channel.Init(("127.0.0.1:" + std::to_string(port)).c_str(), &options) != 0) {
    throw std::runtime_error("failed to init channel to port " + std::to_string(port));
  }
}

TEST(BrpcOutputTransport, BroadcastDeliversToConnectedClient) {
  BroadcastRing ring(1024, 512);
  TopicRegistry topics;
  BrpcOutputTransport transport;
  transport.attach(ring, topics, 100);
  transport.start(28991);
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  brpc::Channel channel;
  connect(channel, 28991);
  Subscription sub = subscribe(channel, "totals");
  // Registration happens on the server's own stream-accept path,
  // slightly after the client's call returns locally — same race as
  // the other two transports' tests, same fix.
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  publishBroadcast(ring, topics, "totals", R"({"sequence_number":1,"total":5})");
  ASSERT_TRUE(waitForCount(sub.handler(), 1, std::chrono::seconds(5)));
  EXPECT_EQ(sub.handler().snapshot().at(0), R"({"sequence_number":1,"total":5})");

  transport.stop();
}

TEST(BrpcOutputTransport, MultipleMessagesArriveInOrder) {
  BroadcastRing ring(1024, 512);
  TopicRegistry topics;
  BrpcOutputTransport transport;
  transport.attach(ring, topics, 100);
  transport.start(28992);
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  brpc::Channel channel;
  connect(channel, 28992);
  Subscription sub = subscribe(channel, "totals");
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  publishBroadcast(ring, topics, "totals", "first");
  publishBroadcast(ring, topics, "totals", "second");
  publishBroadcast(ring, topics, "totals", "third");

  ASSERT_TRUE(waitForCount(sub.handler(), 3, std::chrono::seconds(5)));
  const std::vector<std::string> received = sub.handler().snapshot();
  EXPECT_EQ(received.at(0), "first");
  EXPECT_EQ(received.at(1), "second");
  EXPECT_EQ(received.at(2), "third");

  transport.stop();
}

TEST(BrpcOutputTransport, BroadcastToUnknownTopicIsANoOp) {
  BroadcastRing ring(1024, 512);
  TopicRegistry topics;
  BrpcOutputTransport transport;
  transport.attach(ring, topics, 100);
  transport.start(28993);
  // No client connected at all; this must not crash or hang.
  publishBroadcast(ring, topics, "nobody-subscribed", "hello");
  transport.stop();
}

TEST(BrpcOutputTransport, TwoClientsOnDifferentTopicsOnlyReceiveTheirOwn) {
  BroadcastRing ring(1024, 512);
  TopicRegistry topics;
  BrpcOutputTransport transport;
  transport.attach(ring, topics, 100);
  transport.start(28994);
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  brpc::Channel channel;
  connect(channel, 28994);
  Subscription totalsSub = subscribe(channel, "totals");
  Subscription alertsSub = subscribe(channel, "alerts");
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  publishBroadcast(ring, topics, "totals", "for totals");
  publishBroadcast(ring, topics, "alerts", "for alerts");

  ASSERT_TRUE(waitForCount(totalsSub.handler(), 1, std::chrono::seconds(5)));
  ASSERT_TRUE(waitForCount(alertsSub.handler(), 1, std::chrono::seconds(5)));
  EXPECT_EQ(totalsSub.handler().snapshot().at(0), "for totals");
  EXPECT_EQ(alertsSub.handler().snapshot().at(0), "for alerts");
  // Each subscriber's filter is exclusive, not merely first-match.
  EXPECT_EQ(totalsSub.handler().snapshot().size(), 1u);
  EXPECT_EQ(alertsSub.handler().snapshot().size(), 1u);

  transport.stop();
}

}  // namespace
}  // namespace sequencer
