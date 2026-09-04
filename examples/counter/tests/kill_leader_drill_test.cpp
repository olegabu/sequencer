// specification.md §14's acceptance checklist, items 2 and 5: a real
// kill-leader-under-load drill, and a zone-loss drill (in a one-node-
// per-zone layout, losing any one node's process is losing that zone).
// Three real `counter_node` subprocesses, a real raft group, and a
// client that keeps proposing across the fault exactly as a production
// input gateway would (follow redirects, retry against a different
// endpoint on failure) — then the concrete, journal-level proof of
// §14 item 2's three claims: no journal gaps (dense sequence numbers
// straight through the leader transition), no divergence (every
// surviving — and the killed node's own already-committed — journal
// entry byte-identical), and the new leader's journal continuing
// densely (commits keep succeeding after the kill, not just before it).
//
// What this drill deliberately does NOT attempt: asserting an exact
// expected running total after the kill. examples/counter's
// CounterStateMachine has no idempotency-key deduplication — an
// explicit, documented simplification (see counter_state_machine.hpp's
// class comment) appropriate for the smallest useful example, but one
// that makes "the client blindly resubmitted an input that had, in
// fact, already committed before its acknowledgement was lost" a real
// double-count risk here, unrelated to anything this drill is actually
// meant to test (raft/journal fault tolerance, not this toy state
// machine's request deduplication). So this drill verifies exactly the
// invariants §14 item 2 actually asks for — density, agreement,
// continued progress — never a precomputed sum.

#include "child_process.hpp"

#include <sequencer/temp_dir.hpp>
#include <sequencer/journal/reader.hpp>

#include <brpc/channel.h>
#include <brpc/controller.h>

#include "node.pb.h"

#include <signal.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

namespace sequencer::examples::counter {
namespace {


std::string stripPeerIdIndex(const std::string& peerId) {
  const auto lastColon = peerId.rfind(':');
  return lastColon == std::string::npos ? peerId : peerId.substr(0, lastColon);
}

// A minimal input-gateway-shaped client (specification.md §8.1:
// "forward to the current leader, follow redirects, and on timeout
// resubmit blindly"): caches the last-known leader across calls, like
// gateway/input/src/node_proposer.hpp's NodeProposer, but also reports
// which endpoint actually served each successful call, so the drill
// knows which process to kill.
class LeaderTrackingClient {
 public:
  explicit LeaderTrackingClient(std::vector<std::string> endpoints) : endpoints_(std::move(endpoints)) {}

  struct Outcome {
    bool ok = false;
    std::uint64_t sequenceNumber = 0;
  };

  Outcome propose(std::int64_t delta, std::chrono::seconds deadlineFromNow = std::chrono::seconds(15)) {
    std::size_t index = cachedLeaderIndex_.value_or(0);
    const auto deadline = std::chrono::steady_clock::now() + deadlineFromNow;

    while (std::chrono::steady_clock::now() < deadline) {
      brpc::Channel channel;
      brpc::ChannelOptions options;
      options.timeout_ms = 300;
      options.max_retry = 0;

      if (channel.Init(endpoints_[index].c_str(), &options) == 0) {
        sequencer::node::proto::ProposeService_Stub stub(&channel);
        sequencer::node::proto::ProposeRequest request;
        request.set_input(&delta, sizeof(delta));
        sequencer::node::proto::ProposeResponse response;
        brpc::Controller cntl;
        stub.Propose(&cntl, &request, &response, nullptr);

        if (!cntl.Failed() && !response.redirect() && response.error_message().empty()) {
          cachedLeaderIndex_ = index;
          return Outcome{.ok = true, .sequenceNumber = response.sequence_number()};
        }
        if (!cntl.Failed() && response.redirect() && !response.leader_hint().empty()) {
          const std::string hint = stripPeerIdIndex(response.leader_hint());
          const auto it = std::find(endpoints_.begin(), endpoints_.end(), hint);
          if (it != endpoints_.end()) {
            index = static_cast<std::size_t>(std::distance(endpoints_.begin(), it));
            continue;  // retry immediately against the hinted leader
          }
        }
      }
      cachedLeaderIndex_.reset();
      index = (index + 1) % endpoints_.size();
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    return Outcome{};
  }

  std::optional<std::size_t> cachedLeaderIndex() const { return cachedLeaderIndex_; }

 private:
  std::vector<std::string> endpoints_;
  std::optional<std::size_t> cachedLeaderIndex_;
};

struct Cluster {
  std::vector<std::filesystem::path> dataDirs;
  std::vector<std::unique_ptr<ChildProcess>> nodes;
  std::vector<std::string> channelEndpoints;
};

Cluster startCluster(const std::string& group, int basePort) {
  Cluster cluster;
  std::vector<std::string> raftPeers;
  for (int i = 0; i < 3; ++i) {
    raftPeers.push_back("127.0.0.1:" + std::to_string(basePort + i) + ":0");
    cluster.channelEndpoints.push_back("127.0.0.1:" + std::to_string(basePort + i));
    cluster.dataDirs.push_back(sequencer::makeTempDir("kill_leader_drill"));
  }
  const std::string peersFlag = "--peers=" + raftPeers[0] + "," + raftPeers[1] + "," + raftPeers[2];
  const std::string nodePath = COUNTER_NODE_MAIN_PATH;

  for (int i = 0; i < 3; ++i) {
    // --apply_thread_pure_spin=false: see three_node_smoke_test.cpp's
    // identical flag for why this matters when several node processes
    // share one development machine.
    cluster.nodes.push_back(std::make_unique<ChildProcess>(
        nodePath, std::vector<std::string>{"--group=" + group, "--peer=" + raftPeers[i], peersFlag,
                                            "--data_dir=" + cluster.dataDirs[i].string(),
                                            "--election_timeout_ms=300", "--apply_thread_pure_spin=false"}));
  }
  return cluster;
}

bool waitForCommittedCount(const std::filesystem::path& dataDir, std::uint64_t target,
                            std::chrono::seconds timeout) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    try {
      journal::JournalReader reader(dataDir / "journal");
      if (reader.committedCount() >= target) {
        return true;
      }
    } catch (const std::exception&) {
      // Not created yet on a replica that hasn't received its first entry.
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  return false;
}

// Every entry both journals have in common must be byte-identical —
// specification.md §2.1's "replicas lag, never diverge," checked the
// same way three_node_smoke_test.cpp checks it for an uninterrupted
// cluster.
void expectNoDivergence(const std::filesystem::path& dirA, const std::filesystem::path& dirB) {
  journal::JournalReader a(dirA / "journal");
  journal::JournalReader b(dirB / "journal");
  const std::uint64_t common = std::min(a.committedCount(), b.committedCount());
  ASSERT_GT(common, 0u);
  for (std::uint64_t seq = 1; seq <= common; ++seq) {
    const Payload ra = a.record(seq).rawBytes();
    const Payload rb = b.record(seq).rawBytes();
    ASSERT_EQ(ra.size(), rb.size()) << "seq " << seq;
    EXPECT_TRUE(std::equal(ra.begin(), ra.end(), rb.begin())) << "seq " << seq;
  }
}

TEST(AcceptanceDrills, KillLeaderUnderLoadShowsNoGapsNoDivergenceAndContinuedCommits) {
  Cluster cluster = startCluster("kill-leader-drill", 28900);
  LeaderTrackingClient client(cluster.channelEndpoints);

  // Phase 1: commit a batch to warm up leader discovery — this is the
  // "under load" the leader gets killed during, matching §14 item 2's
  // "with the load generator running, killing the leader process."
  constexpr int kPreKillCommits = 20;
  for (int i = 0; i < kPreKillCommits; ++i) {
    const LeaderTrackingClient::Outcome outcome = client.propose(1);
    ASSERT_TRUE(outcome.ok) << "pre-kill commit " << i << " did not succeed within the retry budget";
  }
  ASSERT_TRUE(client.cachedLeaderIndex().has_value());
  const std::size_t leaderIndex = *client.cachedLeaderIndex();
  ASSERT_TRUE(waitForCommittedCount(cluster.dataDirs[leaderIndex], kPreKillCommits, std::chrono::seconds(5)));

  // The abrupt fault: SIGKILL, not the graceful SIGTERM the ChildProcess
  // destructor uses — no chance for the leader to finish anything.
  cluster.nodes[leaderIndex]->kill(SIGKILL);

  // Phase 2: keep proposing across the fault. §8.1's "on timeout
  // resubmit blindly" is exactly LeaderTrackingClient's retry loop —
  // a call spanning the election window is allowed a longer deadline,
  // since re-election itself takes on the order of seconds (§13).
  constexpr int kPostKillAttempts = 20;
  int postKillSucceeded = 0;
  for (int i = 0; i < kPostKillAttempts; ++i) {
    const LeaderTrackingClient::Outcome outcome = client.propose(1, std::chrono::seconds(10));
    if (outcome.ok) {
      ++postKillSucceeded;
    }
  }
  // A generous allowance for attempts that land exactly inside the
  // election window itself; the substantive claim is that the client
  // recovers and keeps making progress, not that literally zero
  // attempts are ever affected by the fault.
  EXPECT_GE(postKillSucceeded, kPostKillAttempts - 3)
      << "clients must recover and keep committing after the leader is killed (specification.md §14 item 2)";

  std::vector<std::size_t> survivors;
  for (std::size_t i = 0; i < cluster.dataDirs.size(); ++i) {
    if (i != leaderIndex) {
      survivors.push_back(i);
    }
  }
  ASSERT_EQ(survivors.size(), 2u);

  const auto survivorCommittedCount = [&](std::size_t idx) {
    journal::JournalReader reader(cluster.dataDirs[idx] / "journal");
    return reader.committedCount();
  };
  ASSERT_TRUE(waitForCommittedCount(cluster.dataDirs[survivors[0]], kPreKillCommits + 1, std::chrono::seconds(10)))
      << "the new leader's journal must continue densely past the pre-kill count (specification.md §14 item 2)";
  ASSERT_TRUE(waitForCommittedCount(cluster.dataDirs[survivors[1]], survivorCommittedCount(survivors[0]),
                                     std::chrono::seconds(10)));

  // No divergence: the two survivors agree on everything they share,
  // AND the killed leader's own (now-static) journal — everything it
  // had already committed before it died — agrees with them too.
  expectNoDivergence(cluster.dataDirs[survivors[0]], cluster.dataDirs[survivors[1]]);
  expectNoDivergence(cluster.dataDirs[leaderIndex], cluster.dataDirs[survivors[0]]);

  for (const std::filesystem::path& dir : cluster.dataDirs) {
    std::filesystem::remove_all(dir);
  }
}

TEST(AcceptanceDrills, ZoneLossKillingAFollowerLeavesTheSystemOperatingOnTheRemainingTwoZones) {
  // specification.md §13: "one node per availability zone, across
  // three zones... survives the loss of any single zone." In this
  // 3-node, one-node-per-zone layout, killing any one node's process
  // (leader or follower) is exactly a single zone-loss event; this
  // drill covers the follower case (the leader case, the harder one
  // requiring re-election, is the drill above). Latency measurement
  // itself is out of this repository's scope (specification.md §9,
  // §12 — a separate benchmarking repository) — what's verified here
  // is the functional claim: the system keeps operating.
  Cluster cluster = startCluster("zone-loss-drill", 28910);
  LeaderTrackingClient client(cluster.channelEndpoints);

  constexpr int kPreKillCommits = 10;
  for (int i = 0; i < kPreKillCommits; ++i) {
    const LeaderTrackingClient::Outcome outcome = client.propose(1);
    ASSERT_TRUE(outcome.ok) << "pre-kill commit " << i << " did not succeed within the retry budget";
  }
  ASSERT_TRUE(client.cachedLeaderIndex().has_value());
  const std::size_t leaderIndex = *client.cachedLeaderIndex();
  const std::size_t followerToKill = (leaderIndex + 1) % 3;

  cluster.nodes[followerToKill]->kill(SIGKILL);

  // No re-election is needed to lose a follower, so recovery should be
  // fast — this deadline is much tighter than the leader-kill drill's.
  constexpr int kPostKillAttempts = 10;
  int postKillSucceeded = 0;
  for (int i = 0; i < kPostKillAttempts; ++i) {
    const LeaderTrackingClient::Outcome outcome = client.propose(1, std::chrono::seconds(5));
    if (outcome.ok) {
      ++postKillSucceeded;
    }
  }
  EXPECT_GE(postKillSucceeded, kPostKillAttempts - 1)
      << "losing a single zone (one of three nodes) must not meaningfully interrupt service";

  const std::size_t otherSurvivor = 3 - leaderIndex - followerToKill;
  ASSERT_TRUE(waitForCommittedCount(cluster.dataDirs[leaderIndex], kPreKillCommits + 1, std::chrono::seconds(5)));
  ASSERT_TRUE(waitForCommittedCount(cluster.dataDirs[otherSurvivor], kPreKillCommits + 1, std::chrono::seconds(5)));
  expectNoDivergence(cluster.dataDirs[leaderIndex], cluster.dataDirs[otherSurvivor]);
  expectNoDivergence(cluster.dataDirs[followerToKill], cluster.dataDirs[leaderIndex]);

  for (const std::filesystem::path& dir : cluster.dataDirs) {
    std::filesystem::remove_all(dir);
  }
}

}  // namespace
}  // namespace sequencer::examples::counter
