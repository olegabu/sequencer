// End-to-end test of the input gateway chassis (specification.md
// §8.5, §8.6): a real single-node NodeImpl (in-process, self-electing)
// behind a real InputGatewayImpl, submitted to over a real brpc
// channel exactly as an external client would — proving the generic
// chassis loop (accept -> codec->toInput -> verify -> propose ->
// codec->toOutput -> respond) end to end.

#include "input_gateway_impl.hpp"
#include "node_impl.hpp"

#include <sequencer/journal/reader.hpp>

#include <brpc/channel.h>
#include <brpc/controller.h>

#include <algorithm>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

namespace sequencer::gateway::input::detail {
namespace {

// A trivial running-total state machine — the same shape used
// throughout this repository's own tests.
class SumStateMachine : public sequencer::StateMachine {
 public:
  void apply(std::uint64_t, Payload input, OutputCollector& outputs) override {
    std::int64_t delta;
    std::memcpy(&delta, input.data(), sizeof(delta));
    total_ += delta;
    outputs.emit(Payload(reinterpret_cast<const std::byte*>(&total_), sizeof(total_)));
    outputs.designateOutput(0);
  }
  void snapshotSave(sequencer::SnapshotWriter&) override {}
  void snapshotLoad(sequencer::SnapshotReader&) override {}

 private:
  std::int64_t total_ = 0;
};

// Pass-through: the client's raw body IS the 8-byte delta (no JSON, no
// parsing — that's examples/counter's job in a later phase). Echoes
// the sequence number followed by the designated output, so tests can
// check both without a second channel back into the node.
class PassThroughCodec : public sequencer::InputCodec {
 public:
  Result<Bytes> toInput(const ClientRequest& request) override {
    if (request.body.size() != sizeof(std::int64_t)) {
      return Result<Bytes>::Error("expected an 8-byte delta");
    }
    return Result<Bytes>::Ok(Bytes(request.body.begin(), request.body.end()));
  }

  Bytes toOutput(const Receipt& receipt, Payload designatedOutput) override {
    Bytes out(sizeof(receipt.sequenceNumber) + designatedOutput.size());
    std::memcpy(out.data(), &receipt.sequenceNumber, sizeof(receipt.sequenceNumber));
    std::memcpy(out.data() + sizeof(receipt.sequenceNumber), designatedOutput.data(),
                designatedOutput.size());
    return out;
  }

  std::optional<Bytes> onDisconnect(const SessionInfo&) override { return std::nullopt; }
};

Bytes bytesOf(std::int64_t v) {
  Bytes b(sizeof(v));
  std::memcpy(b.data(), &v, sizeof(v));
  return b;
}

std::filesystem::path makeTempDir() {
  std::string tmpl = (std::filesystem::temp_directory_path() / "input_gateway_test_XXXXXX").string();
  if (::mkdtemp(tmpl.data()) == nullptr) {
    throw std::runtime_error("mkdtemp failed");
  }
  return tmpl;
}

class InputGatewayTest : public ::testing::Test {
 protected:
  void SetUp() override {
    dir_ = makeTempDir();

    node::detail::NodeConfig nodeConfig;
    nodeConfig.groupId = "input-gateway-test";
    nodeConfig.peerId = "127.0.0.1:28951:0";
    nodeConfig.initialPeers = nodeConfig.peerId;
    nodeConfig.dataDir = dir_;
    nodeConfig.electionTimeoutMs = 300;

    node_ = std::make_unique<node::detail::NodeImpl>(nodeConfig, std::make_unique<SumStateMachine>());
    node_->start();

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!node_->isLeader() && std::chrono::steady_clock::now() < deadline) {
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    ASSERT_TRUE(node_->isLeader());

    InputGatewayConfig gatewayConfig;
    gatewayConfig.nodeEndpoints = {"127.0.0.1:28951"};
    gatewayConfig.listenPort = 28952;
    gateway_ = std::make_unique<InputGatewayImpl>(gatewayConfig, std::make_unique<PassThroughCodec>(),
                                                   verifier_);
    gateway_->start();

    brpc::ChannelOptions channelOptions;
    channelOptions.timeout_ms = 2000;
    ASSERT_EQ(channel_.Init("127.0.0.1:28952", &channelOptions), 0);
  }

  void TearDown() override {
    gateway_->stop();
    node_->stop();
    std::filesystem::remove_all(dir_);
  }

  // Raw request/response over the generic, empty-schema SubmitService —
  // exactly how any brpc/baidu_std client would drive it.
  struct SubmitResult {
    bool failed = false;
    std::string errorText;
    std::string body;
  };

  SubmitResult submit(Payload requestBody) {
    sequencer::gateway::input::proto::SubmitService_Stub stub(&channel_);
    sequencer::gateway::input::proto::SubmitRequest request;
    sequencer::gateway::input::proto::SubmitResponse response;
    brpc::Controller cntl;
    cntl.request_attachment().append(requestBody.data(), requestBody.size());
    stub.Submit(&cntl, &request, &response, nullptr);

    SubmitResult result;
    result.failed = cntl.Failed();
    if (result.failed) {
      result.errorText = cntl.ErrorText();
    } else {
      result.body = cntl.response_attachment().to_string();
    }
    return result;
  }

  std::filesystem::path dir_;
  std::unique_ptr<node::detail::NodeImpl> node_;
  std::unique_ptr<InputGatewayImpl> gateway_;
  sequencer::SignatureVerifier verifier_ = sequencer::acceptAllSignatures;
  brpc::Channel channel_;
};

TEST_F(InputGatewayTest, SubmitProposesAndReturnsSequenceNumberAndDesignatedOutput) {
  const SubmitResult r1 = submit(bytesOf(5));
  ASSERT_FALSE(r1.failed) << r1.errorText;
  ASSERT_EQ(r1.body.size(), sizeof(std::uint64_t) + sizeof(std::int64_t));
  std::uint64_t seq1;
  std::int64_t total1;
  std::memcpy(&seq1, r1.body.data(), sizeof(seq1));
  std::memcpy(&total1, r1.body.data() + sizeof(seq1), sizeof(total1));
  EXPECT_EQ(seq1, 1u);
  EXPECT_EQ(total1, 5);

  const SubmitResult r2 = submit(bytesOf(-2));
  ASSERT_FALSE(r2.failed) << r2.errorText;
  std::uint64_t seq2;
  std::int64_t total2;
  std::memcpy(&seq2, r2.body.data(), sizeof(seq2));
  std::memcpy(&total2, r2.body.data() + sizeof(seq2), sizeof(total2));
  EXPECT_EQ(seq2, 2u);
  EXPECT_EQ(total2, 3);
}

// Concurrent submits share ProposeBatch RPCs (see node_proposer.hpp's
// proposeAsync comment), which is exactly where a batching bug would
// show up: results are positional, so a mis-ordered or mis-sized batch
// hands a client someone else's sequence number. Every submitter here
// must get a distinct sequence number, and together they must cover
// 1..N with nothing missing and nothing repeated.
TEST_F(InputGatewayTest, ConcurrentSubmitsEachGetTheirOwnSequenceNumber) {
  constexpr int kSubmitters = 24;
  std::vector<std::thread> threads;
  std::mutex resultsMutex;
  std::vector<std::uint64_t> sequenceNumbers;
  std::vector<std::string> failures;

  threads.reserve(kSubmitters);
  for (int i = 0; i < kSubmitters; ++i) {
    threads.emplace_back([&, i] {
      const SubmitResult r = submit(bytesOf(i + 1));
      std::lock_guard<std::mutex> lock(resultsMutex);
      if (r.failed) {
        failures.push_back(r.errorText);
        return;
      }
      std::uint64_t seq;
      std::memcpy(&seq, r.body.data(), sizeof(seq));
      sequenceNumbers.push_back(seq);
    });
  }
  for (std::thread& t : threads) {
    t.join();
  }

  ASSERT_TRUE(failures.empty()) << failures.front();
  ASSERT_EQ(sequenceNumbers.size(), static_cast<std::size_t>(kSubmitters));
  std::sort(sequenceNumbers.begin(), sequenceNumbers.end());
  for (int i = 0; i < kSubmitters; ++i) {
    EXPECT_EQ(sequenceNumbers[static_cast<std::size_t>(i)], static_cast<std::uint64_t>(i + 1))
        << "sequence numbers must be exactly 1.." << kSubmitters << " with no gaps or repeats";
  }

  // And the journal agrees: one record per submit, no more.
  journal::JournalReader reader(dir_ / "journal");
  EXPECT_EQ(reader.committedCount(), static_cast<std::uint64_t>(kSubmitters));
}

TEST_F(InputGatewayTest, MalformedRequestIsRejectedByCodecWithoutProposing) {
  const std::string tooShort = "x";
  const SubmitResult result =
      submit(Payload(reinterpret_cast<const std::byte*>(tooShort.data()), tooShort.size()));
  EXPECT_TRUE(result.failed);

  journal::JournalReader reader(dir_ / "journal");
  EXPECT_EQ(reader.committedCount(), 0u) << "a codec-rejected request must never reach Propose";
}

TEST_F(InputGatewayTest, SignatureVerifierRejectionPreventsProposing) {
  gateway_->stop();
  InputGatewayConfig gatewayConfig;
  gatewayConfig.nodeEndpoints = {"127.0.0.1:28951"};
  gatewayConfig.listenPort = 28952;
  sequencer::SignatureVerifier rejectAll = [](Payload) { return false; };
  gateway_ = std::make_unique<InputGatewayImpl>(gatewayConfig, std::make_unique<PassThroughCodec>(),
                                                 rejectAll);
  gateway_->start();

  const SubmitResult result = submit(bytesOf(5));
  EXPECT_TRUE(result.failed);

  journal::JournalReader reader(dir_ / "journal");
  EXPECT_EQ(reader.committedCount(), 0u) << "a rejected signature must never reach Propose";
}

}  // namespace
}  // namespace sequencer::gateway::input::detail
