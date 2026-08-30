// A single-node braft+brpc integration test: starts one real NodeImpl
// (real disk-backed raft log/meta/snapshot storage, a real brpc server
// on a real port), waits for it to elect itself leader (trivial for a
// one-node group), then drives it with real Propose RPCs over a real
// brpc::Channel and checks the responses and the resulting journal.
//
// A three-node smoke test (leader failover, replication across real
// peers) is specification.md §15 item 3/8's job, paired with the
// counter example's state machine — this test's scope is node/'s own
// mechanics: RunNode's plumbing, sequence-number minting, and deferred
// acknowledgement (§15 item 2), proven end to end on one replica.

#include "node_impl.hpp"

#include <sequencer/journal/reader.hpp>

#include <brpc/channel.h>
#include <brpc/controller.h>

#include <chrono>
#include <cstring>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <thread>

#include <gtest/gtest.h>

#include "node.pb.h"

namespace sequencer::node::detail {
namespace {

// The same running-total shape as specification.md §10's counter
// example, extended with a real (if minimal) snapshot implementation so
// on_snapshot_save/on_snapshot_load are exercised too.
class SumStateMachine : public sequencer::StateMachine {
 public:
  void apply(std::uint64_t, Payload input, OutputCollector& outputs) override {
    std::int64_t delta;
    std::memcpy(&delta, input.data(), sizeof(delta));
    total_ += delta;
    outputs.emit(Payload(reinterpret_cast<const std::byte*>(&total_), sizeof(total_)));
    outputs.designateOutput(0);
  }
  void snapshotSave(sequencer::SnapshotWriter& writer) override { writer.write(&total_, sizeof(total_)); }
  void snapshotLoad(sequencer::SnapshotReader& reader) override { reader.read(&total_, sizeof(total_)); }

 private:
  std::int64_t total_ = 0;
};

class NodeIntegrationTest : public ::testing::Test {
 protected:
  void SetUp() override {
    std::string tmpl =
        (std::filesystem::temp_directory_path() / "node_integration_test_XXXXXX").string();
    ASSERT_NE(::mkdtemp(tmpl.data()), nullptr);
    dir_ = tmpl;

    NodeConfig config;
    config.groupId = "test-group";
    config.peerId = peerAddress_;
    config.initialPeers = peerAddress_;  // single-node group: its own initial configuration
    config.dataDir = dir_;
    config.electionTimeoutMs = 300;  // short, so the single-node leader election below is quick

    node_ = std::make_unique<NodeImpl>(config, std::make_unique<SumStateMachine>());
    node_->start();

    // A single-node group has trivial quorum and should self-elect
    // almost immediately; poll rather than assume a fixed delay.
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!node_->isLeader() && std::chrono::steady_clock::now() < deadline) {
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    ASSERT_TRUE(node_->isLeader()) << "single-node group failed to self-elect within 5s";

    // brpc::Channel wants a plain "ip:port" — unlike braft's PeerId
    // format above, it has no ":idx" suffix.
    brpc::ChannelOptions channelOptions;
    channelOptions.timeout_ms = 2000;
    ASSERT_EQ(channel_.Init("127.0.0.1:28931", &channelOptions), 0);
  }

  void TearDown() override {
    node_->stop();
    node_.reset();
    std::filesystem::remove_all(dir_);
  }

  std::optional<sequencer::node::proto::ProposeResponse> propose(std::int64_t delta) {
    sequencer::node::proto::ProposeService_Stub stub(&channel_);
    sequencer::node::proto::ProposeRequest request;
    request.set_input(&delta, sizeof(delta));
    sequencer::node::proto::ProposeResponse response;
    brpc::Controller cntl;
    stub.Propose(&cntl, &request, &response, nullptr);
    if (cntl.Failed()) {
      return std::nullopt;
    }
    return response;
  }

  std::filesystem::path dir_;
  // A fixed high port for this single test process; see the class
  // comment on why an ephemeral (port 0) bind doesn't fit this design
  // — the raft PeerId's port must be known before the node starts.
  const std::string peerAddress_ = "127.0.0.1:28931:0";
  std::unique_ptr<NodeImpl> node_;
  brpc::Channel channel_;
};

TEST_F(NodeIntegrationTest, ProposeReturnsDenseSequenceNumbersAndDesignatedOutputs) {
  auto r1 = propose(5);
  ASSERT_TRUE(r1.has_value());
  EXPECT_TRUE(r1->error_message().empty());
  EXPECT_FALSE(r1->redirect());
  EXPECT_EQ(r1->sequence_number(), 1u);
  ASSERT_EQ(r1->designated_output().size(), sizeof(std::int64_t));
  std::int64_t total1;
  std::memcpy(&total1, r1->designated_output().data(), sizeof(total1));
  EXPECT_EQ(total1, 5);

  auto r2 = propose(-2);
  ASSERT_TRUE(r2.has_value());
  EXPECT_EQ(r2->sequence_number(), 2u);
  std::int64_t total2;
  std::memcpy(&total2, r2->designated_output().data(), sizeof(total2));
  EXPECT_EQ(total2, 3);

  auto r3 = propose(10);
  ASSERT_TRUE(r3.has_value());
  EXPECT_EQ(r3->sequence_number(), 3u);
}

TEST_F(NodeIntegrationTest, ProposedInputsAreDurablyJournaled) {
  ASSERT_TRUE(propose(1).has_value());
  ASSERT_TRUE(propose(2).has_value());
  ASSERT_TRUE(propose(3).has_value());

  journal::JournalReader reader(dir_ / "journal");
  ASSERT_EQ(reader.committedCount(), 3u);
  for (std::uint64_t seq = 1; seq <= 3; ++seq) {
    journal::RecordView r = reader.record(seq);
    EXPECT_EQ(r.sequenceNumber(), seq);
    std::int64_t delta;
    std::memcpy(&delta, r.input().data(), sizeof(delta));
    EXPECT_EQ(delta, static_cast<std::int64_t>(seq));
    ASSERT_EQ(r.outputCount(), 1u);
  }
}

}  // namespace
}  // namespace sequencer::node::detail
