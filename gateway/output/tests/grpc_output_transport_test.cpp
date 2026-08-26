// Tests for GrpcOutputTransport in isolation — a real gRPC C++ client
// connects over a real socket, and the test publishes tagged entries
// into a BroadcastRing the transport is attached to (exactly how the
// chassis's RingFanout does it), verifying actual network delivery
// through the per-subscriber reader path (not just that the code
// compiles against gRPC's API). Mirrors
// websocket_output_transport_test.cpp's shape exactly.

#include <sequencer/grpc_output_transport.hpp>

#include <grpcpp/grpcpp.h>

#include "output_grpc.grpc.pb.h"

#include <chrono>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

namespace sequencer {
namespace {

class TestGrpcClient {
 public:
  TestGrpcClient(int port, const std::string& topic) {
    auto channel = grpc::CreateChannel("127.0.0.1:" + std::to_string(port), grpc::InsecureChannelCredentials());
    stub_ = gateway::output::grpc_proto::GenericOutputService::NewStub(channel);
    gateway::output::grpc_proto::SubscribeRequest request;
    request.set_topic(topic);
    reader_ = stub_->Subscribe(&context_, request);
  }

  // Reads span whatever batch shape the server happened to send them
  // in (see grpc_output_transport.cpp's own batching comment) — doles
  // out one payload at a time regardless, pulling a fresh
  // OutputRecordBatch off the stream whenever the buffered one is
  // exhausted.
  std::string readOne() {
    if (bufferPos_ >= buffered_.size()) {
      gateway::output::grpc_proto::OutputRecordBatch batch;
      if (!reader_->Read(&batch)) {
        throw std::runtime_error("stream ended before a record arrived");
      }
      buffered_.assign(batch.payloads().begin(), batch.payloads().end());
      bufferPos_ = 0;
      if (buffered_.empty()) {
        throw std::runtime_error("server sent an empty OutputRecordBatch");
      }
    }
    return buffered_[bufferPos_++];
  }

 private:
  grpc::ClientContext context_;
  std::unique_ptr<gateway::output::grpc_proto::GenericOutputService::Stub> stub_;
  std::unique_ptr<grpc::ClientReader<gateway::output::grpc_proto::OutputRecordBatch>> reader_;
  std::vector<std::string> buffered_;
  std::size_t bufferPos_ = 0;
};

// Publishes exactly the way OutputGatewayImpl's RingFanout does —
// tagged with the interned topic id; readers filter on it.
void publishBroadcast(BroadcastRing& ring, TopicRegistry& topics, const std::string& topic,
                      const std::string& payload) {
  ring.publish(makeTopicTag(topics.idFor(topic)), reinterpret_cast<const std::byte*>(payload.data()),
               payload.size());
}

TEST(GrpcOutputTransport, BroadcastDeliversToConnectedClient) {
  BroadcastRing ring(1024, 512);
  TopicRegistry topics;
  GrpcOutputTransport transport;
  transport.attach(ring, topics, 100);
  transport.start(28981);
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  TestGrpcClient client(28981, "totals");
  // Subscribe()'s registration happens on the server's own call
  // thread, slightly after the client's stub call returns locally —
  // same race as WebSocketOutputTransport's tests, same fix.
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  publishBroadcast(ring, topics, "totals", R"({"sequence_number":1,"total":5})");
  EXPECT_EQ(client.readOne(), R"({"sequence_number":1,"total":5})");

  transport.stop();
}

TEST(GrpcOutputTransport, MultipleMessagesArriveInOrder) {
  BroadcastRing ring(1024, 512);
  TopicRegistry topics;
  GrpcOutputTransport transport;
  transport.attach(ring, topics, 100);
  transport.start(28982);
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  TestGrpcClient client(28982, "totals");
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  publishBroadcast(ring, topics, "totals", "first");
  publishBroadcast(ring, topics, "totals", "second");
  publishBroadcast(ring, topics, "totals", "third");

  EXPECT_EQ(client.readOne(), "first");
  EXPECT_EQ(client.readOne(), "second");
  EXPECT_EQ(client.readOne(), "third");

  transport.stop();
}

TEST(GrpcOutputTransport, BroadcastToUnknownTopicIsANoOp) {
  BroadcastRing ring(1024, 512);
  TopicRegistry topics;
  GrpcOutputTransport transport;
  transport.attach(ring, topics, 100);
  transport.start(28983);
  // No client connected at all; this must not crash or hang.
  publishBroadcast(ring, topics, "nobody-subscribed", "hello");
  transport.stop();
}

TEST(GrpcOutputTransport, TwoClientsOnDifferentTopicsOnlyReceiveTheirOwn) {
  BroadcastRing ring(1024, 512);
  TopicRegistry topics;
  GrpcOutputTransport transport;
  transport.attach(ring, topics, 100);
  transport.start(28984);
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  TestGrpcClient totalsClient(28984, "totals");
  TestGrpcClient alertsClient(28984, "alerts");
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  publishBroadcast(ring, topics, "totals", "for totals");
  publishBroadcast(ring, topics, "alerts", "for alerts");

  EXPECT_EQ(totalsClient.readOne(), "for totals");
  EXPECT_EQ(alertsClient.readOne(), "for alerts");

  transport.stop();
}

}  // namespace
}  // namespace sequencer
