// specification.md §15 item 3: the three-local-node smoke test. Three
// real `counter_node` subprocesses, a real raft group, real Propose
// RPCs over the network, following real leader redirects exactly as a
// gateway would (§8.1) — and, the actual point of testing three nodes
// rather than one, a byte-for-byte comparison of all three replicas'
// journals: the concrete proof behind "replicas lag, never diverge" (§3).

#include "child_process.hpp"

#include <sequencer/journal/reader.hpp>

#include <brpc/channel.h>
#include <brpc/controller.h>

#include "node.pb.h"

#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

namespace sequencer::examples::counter {
namespace {

std::filesystem::path makeTempDir() {
  std::string tmpl = (std::filesystem::temp_directory_path() / "counter_smoke_XXXXXX").string();
  if (::mkdtemp(tmpl.data()) == nullptr) {
    throw std::runtime_error("mkdtemp failed");
  }
  return tmpl;
}

// braft's PeerId format ("ip:port:idx") isn't a valid brpc::Channel
// endpoint ("ip:port") — strip the trailing index.
std::string stripPeerIdIndex(const std::string& peerId) {
  const auto lastColon = peerId.rfind(':');
  return lastColon == std::string::npos ? peerId : peerId.substr(0, lastColon);
}

struct ProposeOutcome {
  bool ok = false;
  std::uint64_t sequenceNumber = 0;
  std::int64_t designatedTotal = 0;
};

// Proposes `delta`, following redirects (specification.md §3:
// "Propose(bytes) -> {...} | redirect(leader) | error") exactly as an
// input gateway would (§8.1: "forward to the current leader, follow
// redirects, and on timeout resubmit blindly"), including the window
// right after startup where no leader has been elected yet at all.
ProposeOutcome proposeFollowingRedirects(const std::vector<std::string>& endpoints, std::int64_t delta) {
  std::string target = endpoints.front();
  std::size_t nextEndpointIndex = 1 % endpoints.size();
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(15);

  while (std::chrono::steady_clock::now() < deadline) {
    brpc::Channel channel;
    brpc::ChannelOptions channelOptions;
    channelOptions.timeout_ms = 500;
    channelOptions.max_retry = 0;

    bool retryDifferentEndpoint = false;
    if (channel.Init(target.c_str(), &channelOptions) == 0) {
      sequencer::node::proto::ProposeService_Stub stub(&channel);
      sequencer::node::proto::ProposeRequest request;
      request.set_input(&delta, sizeof(delta));
      sequencer::node::proto::ProposeResponse response;
      brpc::Controller cntl;
      stub.Propose(&cntl, &request, &response, nullptr);

      if (!cntl.Failed() && !response.redirect() && response.error_message().empty()) {
        ProposeOutcome outcome;
        outcome.ok = true;
        outcome.sequenceNumber = response.sequence_number();
        if (response.designated_output().size() == sizeof(std::int64_t)) {
          std::memcpy(&outcome.designatedTotal, response.designated_output().data(),
                      sizeof(std::int64_t));
        }
        return outcome;
      }
      if (!cntl.Failed() && response.redirect() && !response.leader_hint().empty()) {
        target = stripPeerIdIndex(response.leader_hint());
      } else {
        retryDifferentEndpoint = true;
      }
    } else {
      retryDifferentEndpoint = true;
    }

    if (retryDifferentEndpoint) {
      target = endpoints[nextEndpointIndex];
      nextEndpointIndex = (nextEndpointIndex + 1) % endpoints.size();
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  return {};
}

bool waitForCommittedCount(const std::filesystem::path& dataDir, std::uint64_t target,
                            std::chrono::seconds timeout) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    try {
      journal::JournalReader reader(dataDir / "journal.data", dataDir / "journal.index");
      if (reader.committedCount() >= target) {
        return true;
      }
    } catch (const std::exception&) {
      // The journal file pair may not exist yet on a replica that
      // hasn't received its first entry.
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  return false;
}

TEST(ThreeNodeSmoke, ReplicatesIdenticallyAcrossAllThreeJournals) {
  const std::filesystem::path dir0 = makeTempDir();
  const std::filesystem::path dir1 = makeTempDir();
  const std::filesystem::path dir2 = makeTempDir();

  const std::vector<std::string> raftPeers = {"127.0.0.1:28941:0", "127.0.0.1:28942:0",
                                               "127.0.0.1:28943:0"};
  const std::string peersFlag = "--peers=" + raftPeers[0] + "," + raftPeers[1] + "," + raftPeers[2];
  const std::string nodePath = COUNTER_NODE_MAIN_PATH;

  // --apply_thread_pure_spin=false: three node processes sharing one
  // test machine would otherwise each unconditionally peg a core
  // (specification.md §5.4), starving the raft replication threads
  // leader election itself depends on. See apply_loop.hpp's run() and
  // run_node.cpp's flag definition for the full rationale — this is a
  // local-development concession only, never a production default.
  ChildProcess node0(nodePath, {"--group=counter-smoke", "--peer=" + raftPeers[0], peersFlag,
                                 "--data_dir=" + dir0.string(), "--election_timeout_ms=300",
                                 "--apply_thread_pure_spin=false"});
  ChildProcess node1(nodePath, {"--group=counter-smoke", "--peer=" + raftPeers[1], peersFlag,
                                 "--data_dir=" + dir1.string(), "--election_timeout_ms=300",
                                 "--apply_thread_pure_spin=false"});
  ChildProcess node2(nodePath, {"--group=counter-smoke", "--peer=" + raftPeers[2], peersFlag,
                                 "--data_dir=" + dir2.string(), "--election_timeout_ms=300",
                                 "--apply_thread_pure_spin=false"});

  const std::vector<std::string> channelEndpoints = {"127.0.0.1:28941", "127.0.0.1:28942",
                                                       "127.0.0.1:28943"};

  const std::vector<std::int64_t> deltas = {5, -2, 10, -13, 100};
  std::int64_t expectedTotal = 0;
  std::uint64_t previousSeq = 0;
  for (std::int64_t delta : deltas) {
    const ProposeOutcome outcome = proposeFollowingRedirects(channelEndpoints, delta);
    ASSERT_TRUE(outcome.ok) << "Propose did not succeed within the retry budget";
    expectedTotal += delta;
    EXPECT_EQ(outcome.sequenceNumber, previousSeq + 1);
    EXPECT_EQ(outcome.designatedTotal, expectedTotal);
    previousSeq = outcome.sequenceNumber;
  }

  const auto total = static_cast<std::uint64_t>(deltas.size());
  ASSERT_TRUE(waitForCommittedCount(dir0, total, std::chrono::seconds(5)));
  ASSERT_TRUE(waitForCommittedCount(dir1, total, std::chrono::seconds(5)));
  ASSERT_TRUE(waitForCommittedCount(dir2, total, std::chrono::seconds(5)));

  journal::JournalReader reader0(dir0 / "journal.data", dir0 / "journal.index");
  journal::JournalReader reader1(dir1 / "journal.data", dir1 / "journal.index");
  journal::JournalReader reader2(dir2 / "journal.data", dir2 / "journal.index");

  for (std::uint64_t seq = 1; seq <= total; ++seq) {
    const Payload r0 = reader0.record(seq).rawBytes();
    const Payload r1 = reader1.record(seq).rawBytes();
    const Payload r2 = reader2.record(seq).rawBytes();
    ASSERT_EQ(r0.size(), r1.size()) << "seq " << seq;
    ASSERT_EQ(r0.size(), r2.size()) << "seq " << seq;
    EXPECT_TRUE(std::equal(r0.begin(), r0.end(), r1.begin())) << "seq " << seq << ": node0 vs node1";
    EXPECT_TRUE(std::equal(r0.begin(), r0.end(), r2.begin())) << "seq " << seq << ": node0 vs node2";
  }

  std::filesystem::remove_all(dir0);
  std::filesystem::remove_all(dir1);
  std::filesystem::remove_all(dir2);
}

}  // namespace
}  // namespace sequencer::examples::counter
