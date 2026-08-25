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

  // Reads span whatever batch shape the server happened to send them
  // in (see relay_grpc_service_impl.hpp's adaptive batching) — this
  // doles out one record at a time regardless, pulling a fresh
  // RecordBatch off the stream whenever the buffered one is exhausted.
  std::int64_t readOne() {
    if (bufferPos_ >= buffered_.size()) {
      grpc_proto::RecordBatch batch;
      if (!reader_->Read(&batch)) {
        throw std::runtime_error("stream ended before a record arrived");
      }
      buffered_.assign(batch.raw_records().begin(), batch.raw_records().end());
      bufferPos_ = 0;
      if (buffered_.empty()) {
        throw std::runtime_error("server sent an empty RecordBatch");
      }
    }
    const std::string& rawRecord = buffered_[bufferPos_++];
    const journal::RecordView view(reinterpret_cast<const std::byte*>(rawRecord.data()),
                                    static_cast<std::uint32_t>(rawRecord.size()));
    std::int64_t value;
    std::memcpy(&value, view.input().data(), sizeof(value));
    return value;
  }

 private:
  grpc::ClientContext context_;
  std::unique_ptr<grpc_proto::RelayService::Stub> stub_;
  std::unique_ptr<grpc::ClientReader<grpc_proto::RecordBatch>> reader_;
  std::vector<std::string> buffered_;
  std::size_t bufferPos_ = 0;
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

TEST(RelayGrpc, DeliversRecordsAppendedContinuouslyWhileSubscribed) {
  const std::filesystem::path dir = makeTempDir();
  appendRecords(dir, 1, {1});  // seed the journal so a JournalReader can open before appending live

  RelayGatewayConfig config;
  config.dataDir = dir;
  config.listenPort = 0;
  RelayGatewayImpl gateway(std::move(config));
  gateway.start();

  RelayGrpcServiceImpl service(gateway);
  grpc::ServerBuilder builder;
  builder.AddListeningPort("127.0.0.1:29063", grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<grpc::Server> server = builder.BuildAndStart();
  ASSERT_NE(server, nullptr);

  TestGrpcRelayClient client(29063, /*fromSequenceNumber=*/0);
  EXPECT_EQ(client.readOne(), 1);

  // A background writer appends continuously and independently of the
  // reader — not lockstep — reproducing the shape of live traffic
  // racing the tailing/gather loop (relay_grpc_service_impl.hpp)
  // rather than a single batch of pre-existing history the gather
  // loop finds all at once before Subscribe() even starts reading.
  constexpr int kAppended = 20000;
  std::thread writerThread([&dir] {
    journal::JournalWriter writer(dir / "journal.data", dir / "journal.index");
    for (int i = 0; i < kAppended; ++i) {
      writer.append(2 + i, payloadOf(1000 + i), {});
      if (i % 64 == 0) {
        writer.flush(false);
      }
    }
    writer.flush(false);
  });

  for (int i = 0; i < kAppended; ++i) {
    EXPECT_EQ(client.readOne(), 1000 + i) << "at i=" << i;
  }
  writerThread.join();

  service.requestStop();
  server->Shutdown(std::chrono::system_clock::now() + std::chrono::seconds(5));
  server->Wait();
  gateway.stop();
  std::filesystem::remove_all(dir);
}

// A regression guard, not a precise benchmark (raft-tests/ owns real
// fleet numbers): this specific shape — a large pre-existing backlog,
// drained as fast as the gather loop and network allow — is exactly
// what once regressed catastrophically (a stray context->IsCancelled()
// inside the per-record gather loop, ~3000x slower than the fix; see
// gateway/relay/README.md's "Batching the gRPC stream" section). A
// generous bound (two orders of magnitude below what a healthy build
// achieves locally) catches a repeat of that class of bug without
// being sensitive to ordinary machine-to-machine variance.
TEST(RelayGrpc, BacklogCatchUpThroughputStaysOffTheGatherLoopFloor) {
  const std::filesystem::path dir = makeTempDir();
  constexpr int kBacklog = 50000;
  {
    journal::JournalWriter writer(dir / "journal.data", dir / "journal.index");
    for (int i = 0; i < kBacklog; ++i) {
      writer.append(1 + i, payloadOf(i), {});
      if (i % 256 == 0) writer.flush(false);
    }
    writer.flush(false);
  }

  RelayGatewayConfig config;
  config.dataDir = dir;
  config.listenPort = 0;
  RelayGatewayImpl gateway(std::move(config));
  gateway.start();

  RelayGrpcServiceImpl service(gateway);
  grpc::ServerBuilder builder;
  builder.AddListeningPort("127.0.0.1:29064", grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<grpc::Server> server = builder.BuildAndStart();
  ASSERT_NE(server, nullptr);

  const auto t0 = std::chrono::steady_clock::now();
  TestGrpcRelayClient client(29064, /*fromSequenceNumber=*/0);
  for (int i = 0; i < kBacklog; ++i) {
    EXPECT_EQ(client.readOne(), i) << "at i=" << i;
  }
  const auto t1 = std::chrono::steady_clock::now();
  const double secs = std::chrono::duration<double>(t1 - t0).count();
  std::fprintf(stderr, "drained %d backlog records in %.3fs (%.0f records/sec)\n", kBacklog, secs, kBacklog / secs);
  // A healthy debug build drains comfortably over 100k/s locally (a
  // release build: low millions/s); the old per-record IsCancelled()
  // bug measured under 200/s. 5s for 50k records (10k/s) is a
  // generous floor on either build type.
  EXPECT_LT(secs, 5.0);

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
