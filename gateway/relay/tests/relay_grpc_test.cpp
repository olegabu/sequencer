// End-to-end test of the relay gateway's real-gRPC service
// (relay_grpc_service_impl.hpp) — a real gRPC C++ client against a
// directly-synthesized journal, mirroring relay_gateway_test.cpp's
// brpc-based coverage for the same contract (specification.md §8.2).

#include "relay_gateway_impl.hpp"
#include "relay_grpc_service_impl.hpp"

#include <sequencer/journal/writer.hpp>

#include <grpcpp/grpcpp.h>

#include <chrono>
#include <cstring>
#include <filesystem>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

namespace sequencer::gateway::relay::detail {
namespace {

std::filesystem::path makeTempDir() {
  std::string tmpl = (std::filesystem::temp_directory_path() / "relay_grpc_test_XXXXXX").string();
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

class TestGrpcRelayClient {
 public:
  TestGrpcRelayClient(int port, std::uint64_t fromSequenceNumber) {
    auto channel = grpc::CreateChannel("127.0.0.1:" + std::to_string(port), grpc::InsecureChannelCredentials());
    stub_ = grpc_proto::RelayService::NewStub(channel);
    grpc_proto::SubscribeRequest request;
    request.set_from_sequence_number(fromSequenceNumber);
    reader_ = stub_->Subscribe(&context_, request);
  }

  std::int64_t readOne() {
    grpc_proto::Record record;
    if (!reader_->Read(&record)) {
      throw std::runtime_error("stream ended before a record arrived");
    }
    const journal::RecordView view(reinterpret_cast<const std::byte*>(record.raw_record().data()),
                                    static_cast<std::uint32_t>(record.raw_record().size()));
    std::int64_t value;
    std::memcpy(&value, view.input().data(), sizeof(value));
    return value;
  }

 private:
  grpc::ClientContext context_;
  std::unique_ptr<grpc_proto::RelayService::Stub> stub_;
  std::unique_ptr<grpc::ClientReader<grpc_proto::Record>> reader_;
};

TEST(RelayGrpc, SubscribingFromTheBeginningReplaysAlreadyCommittedHistory) {
  const std::filesystem::path dir = makeTempDir();
  appendRecords(dir, 1, {5, -2, 10, -13, 100});

  RelayGatewayConfig config;
  config.dataDir = dir;
  config.listenPort = 0;  // this test only serves grpc, not the brpc-based service
  RelayGatewayImpl gateway(std::move(config));
  gateway.start();

  RelayGrpcServiceImpl service(gateway);
  grpc::ServerBuilder builder;
  builder.AddListeningPort("127.0.0.1:29061", grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<grpc::Server> server = builder.BuildAndStart();
  ASSERT_NE(server, nullptr);

  TestGrpcRelayClient client(29061, /*fromSequenceNumber=*/0);
  const std::vector<std::int64_t> expected = {5, -2, 10, -13, 100};
  for (std::int64_t v : expected) {
    EXPECT_EQ(client.readOne(), v);
  }

  // requestStop() wakes the in-flight Subscribe() call directly;
  // Shutdown()'s bounded deadline is just a backstop.
  service.requestStop();
  server->Shutdown(std::chrono::system_clock::now() + std::chrono::seconds(5));
  server->Wait();
  gateway.stop();
  std::filesystem::remove_all(dir);
}

TEST(RelayGrpc, EachSubscriberHasAnIndependentCursorFromItsOwnRequestedSequenceNumber) {
  const std::filesystem::path dir = makeTempDir();
  appendRecords(dir, 1, {10, 20, 30, 40, 50});

  RelayGatewayConfig config;
  config.dataDir = dir;
  config.listenPort = 0;
  RelayGatewayImpl gateway(std::move(config));
  gateway.start();

  RelayGrpcServiceImpl service(gateway);
  grpc::ServerBuilder builder;
  builder.AddListeningPort("127.0.0.1:29062", grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<grpc::Server> server = builder.BuildAndStart();
  ASSERT_NE(server, nullptr);

  TestGrpcRelayClient fromStart(29062, 0);
  TestGrpcRelayClient fromMiddle(29062, 3);

  EXPECT_EQ(fromStart.readOne(), 10);
  EXPECT_EQ(fromMiddle.readOne(), 30);
  EXPECT_EQ(fromStart.readOne(), 20);
  EXPECT_EQ(fromMiddle.readOne(), 40);

  // requestStop() wakes the in-flight Subscribe() call directly;
  // Shutdown()'s bounded deadline is just a backstop.
  service.requestStop();
  server->Shutdown(std::chrono::system_clock::now() + std::chrono::seconds(5));
  server->Wait();
  gateway.stop();
  std::filesystem::remove_all(dir);
}

}  // namespace
}  // namespace sequencer::gateway::relay::detail
