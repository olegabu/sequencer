#include <sequencer/node.hpp>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <atomic>
#include <chrono>
#include <csignal>
#include <thread>

#include "node_impl.hpp"

DEFINE_string(group, "sequencer", "Raft group id shared by every replica in this deployment");
DEFINE_string(peer, "", "This replica's own address, host:port[:idx] (required)");
DEFINE_string(peers, "",
              "Comma-separated initial peer list, host:port[:idx],... — only consulted when "
              "bootstrapping a brand-new group (empty raft log and meta)");
DEFINE_string(data_dir, "",
              "Directory for this replica's journal, raft log, raft meta, and snapshots (required)");
DEFINE_int32(election_timeout_ms, 1000, "specification.md §5.3 default: never lowered across zones");
DEFINE_int32(apply_thread_cpu, -1,
             "CPU core to pin the pinned apply thread to, or -1 to not pin "
             "(specification.md §5.3's recommended production layout: a dedicated core with its "
             "hyperthread sibling left empty)");
DEFINE_bool(apply_thread_pure_spin, true,
            "specification.md §5.4: production default is true (busy-spin unconditionally). "
            "Set to false only for local development running several node processes on one "
            "small machine, where multiple unconditionally-spinning apply threads would "
            "otherwise starve the raft replication threads leader election depends on — never "
            "set false in production or in any published measurement.");

namespace sequencer {
namespace {

std::atomic<bool> gStopRequested{false};

// Signal-handler-safe: only sets a flag. The actual shutdown sequence
// (stop the apply thread, shut down the raft node, stop the brpc
// server) runs on the ordinary thread that started them, below.
void handleStopSignal(int /*signum*/) { gStopRequested.store(true, std::memory_order_relaxed); }

}  // namespace

int RunNode(int argc, char** argv, std::unique_ptr<StateMachine> stateMachine) {
  google::InitGoogleLogging(argv[0]);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  if (FLAGS_peer.empty() || FLAGS_data_dir.empty()) {
    LOG(ERROR) << "RunNode: --peer and --data_dir are required";
    return 1;
  }

  node::detail::NodeConfig config;
  config.groupId = FLAGS_group;
  config.peerId = FLAGS_peer;
  config.initialPeers = FLAGS_peers;
  config.dataDir = FLAGS_data_dir;
  config.electionTimeoutMs = FLAGS_election_timeout_ms;
  config.applyThreadCpu = FLAGS_apply_thread_cpu;
  config.applyThreadPureSpin = FLAGS_apply_thread_pure_spin;

  node::detail::NodeImpl nodeImpl(std::move(config), std::move(stateMachine));
  nodeImpl.start();
  LOG(INFO) << "node started: group=" << FLAGS_group << " peer=" << FLAGS_peer
            << " data_dir=" << FLAGS_data_dir;

  std::signal(SIGINT, handleStopSignal);
  std::signal(SIGTERM, handleStopSignal);
  while (!gStopRequested.load(std::memory_order_relaxed)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }

  LOG(INFO) << "node stopping";
  nodeImpl.stop();
  return 0;
}

}  // namespace sequencer
