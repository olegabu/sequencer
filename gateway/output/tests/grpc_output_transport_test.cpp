// Tests for GrpcOutputTransport in isolation — a real gRPC C++ client
// connects over a real socket, and the test drives
// Fanout::broadcast/toSession directly, verifying actual network
// delivery (not just that the code compiles against gRPC's API).
// Mirrors websocket_output_transport_test.cpp's shape exactly.

#include <sequencer/grpc_output_transport.hpp>

#include <grpcpp/grpcpp.h>

#include "output_grpc.grpc.pb.h"

#include <chrono>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>

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

  std::string readOne() {
    gateway::output::grpc_proto::OutputRecord record;
    if (!reader_->Read(&record)) {
      throw std::runtime_error("stream ended before a record arrived");
    }
    return record.payload();
  }

 private:
  grpc::ClientContext context_;
  std::unique_ptr<gateway::output::grpc_proto::GenericOutputService::Stub> stub_;
  std::unique_ptr<grpc::ClientReader<gateway::output::grpc_proto::OutputRecord>> reader_;
};

Bytes bytesOf(const std::string& s) {
  return Bytes(reinterpret_cast<const std::byte*>(s.data()), reinterpret_cast<const std::byte*>(s.data()) + s.size());
}

TEST(GrpcOutputTransport, BroadcastDeliversToConnectedClient) {
  GrpcOutputTransport transport;
  transport.start(28981);
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  TestGrpcClient client(28981, "totals");
  // Subscribe()'s registration happens on the server's own call
  // thread, slightly after the client's stub call returns locally —
  // same race as WebSocketOutputTransport's tests, same fix.
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  transport.broadcast("totals", bytesOf(R"({"sequence_number":1,"total":5})"));
  EXPECT_EQ(client.readOne(), R"({"sequence_number":1,"total":5})");

  transport.stop();
}

TEST(GrpcOutputTransport, MultipleMessagesArriveInOrder) {
  GrpcOutputTransport transport;
  transport.start(28982);
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  TestGrpcClient client(28982, "totals");
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  transport.broadcast("totals", bytesOf("first"));
  transport.broadcast("totals", bytesOf("second"));
  transport.broadcast("totals", bytesOf("third"));

  EXPECT_EQ(client.readOne(), "first");
  EXPECT_EQ(client.readOne(), "second");
  EXPECT_EQ(client.readOne(), "third");

  transport.stop();
}

TEST(GrpcOutputTransport, BroadcastToUnknownTopicIsANoOp) {
  GrpcOutputTransport transport;
  transport.start(28983);
  // No client connected at all; this must not crash or hang.
  transport.broadcast("nobody-subscribed", bytesOf("hello"));
  transport.stop();
}

TEST(GrpcOutputTransport, TwoClientsOnDifferentTopicsOnlyReceiveTheirOwn) {
  GrpcOutputTransport transport;
  transport.start(28984);
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  TestGrpcClient totalsClient(28984, "totals");
  TestGrpcClient alertsClient(28984, "alerts");
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  transport.broadcast("totals", bytesOf("for totals"));
  transport.broadcast("alerts", bytesOf("for alerts"));

  EXPECT_EQ(totalsClient.readOne(), "for totals");
  EXPECT_EQ(alertsClient.readOne(), "for alerts");

  transport.stop();
}

}  // namespace
}  // namespace sequencer
