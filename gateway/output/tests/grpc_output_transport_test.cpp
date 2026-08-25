// Tests for GrpcOutputTransport in isolation — a real gRPC C++ client
// connects over a real socket, and the test drives
// Fanout::broadcast/toSession directly, verifying actual network
// delivery (not just that the code compiles against gRPC's API).
// Mirrors websocket_output_transport_test.cpp's shape exactly.

#include <sequencer/grpc_output_transport.hpp>

#include <grpcpp/grpcpp.h>

#include "output_grpc.grpc.pb.h"

#include <atomic>
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

  void cancel() { context_.TryCancel(); }

 private:
  grpc::ClientContext context_;
  std::unique_ptr<gateway::output::grpc_proto::GenericOutputService::Stub> stub_;
  std::unique_ptr<grpc::ClientReader<gateway::output::grpc_proto::OutputRecordBatch>> reader_;
  std::vector<std::string> buffered_;
  std::size_t bufferPos_ = 0;
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

// Regression guard for OutputSubscribeReactor's own lifecycle
// (grpc_output_transport.cpp): repeated subscribe/cancel racing a
// continuous stream of live broadcast()s must never crash or hang —
// no crash, no hang, and the transport stays fully usable afterward.
// Mirrors gateway/relay/tests/relay_grpc_test.cpp's own
// SurvivesClientsCancellingMidStreamRepeatedly exactly, adapted for
// this transport's push (broadcast()) rather than pull (journal-tail)
// shape — the writer thread calls broadcast() directly instead of
// appending to a journal.
TEST(GrpcOutputTransport, SurvivesClientsCancellingMidStreamRepeatedly) {
  GrpcOutputTransport transport;
  transport.start(28985);
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  std::atomic<bool> stopWriting{false};
  std::thread writerThread([&transport, &stopWriting] {
    int i = 0;
    while (!stopWriting.load(std::memory_order_relaxed)) {
      transport.broadcast("totals", bytesOf("msg-" + std::to_string(i++)));
      std::this_thread::sleep_for(std::chrono::microseconds(50));
    }
  });

  for (int round = 0; round < 30; ++round) {
    TestGrpcClient client(28985, "totals");
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    // A handful of reads first, so cancellation actually lands
    // mid-stream (with live broadcasts racing it) rather than before
    // the reactor has done anything at all.
    try {
      for (int i = 0; i < 3; ++i) {
        client.readOne();
      }
    } catch (const std::runtime_error&) {
      // A slow round can legitimately race the writer thread's own
      // pacing; this test is about surviving cancellation cleanly; not
      // about guaranteeing 3 reads always land in time.
    }
    client.cancel();
  }

  stopWriting.store(true, std::memory_order_relaxed);
  writerThread.join();

  // The service must still be fully usable after all that cancelling.
  TestGrpcClient freshClient(28985, "totals");
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  transport.broadcast("totals", bytesOf("still-alive"));
  EXPECT_EQ(freshClient.readOne(), "still-alive");

  transport.stop();
}

}  // namespace
}  // namespace sequencer
