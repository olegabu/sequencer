#pragma once

// Ties together the braft node, the brpc server, the journal, the
// committed-entry ring, and the pinned apply thread — everything
// RunNode() (node.hpp) sets up, minus argv/gflags parsing and signal
// handling, which stay in run_node.cpp so this class can be driven
// directly and deterministically from tests (node/tests/node_test.cpp).

#include <braft/raft.h>
#include <brpc/server.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>

#include <sequencer/journal/writer.hpp>
#include <sequencer/state_machine.hpp>

#include "apply_loop.hpp"
#include "committed_entry_ring.hpp"
#include "raft/propose_service_impl.hpp"
#include "raft/raft_state_machine_adapter.hpp"

namespace sequencer::node::detail {

struct NodeConfig {
  std::string groupId;
  std::string peerId;        // this replica's own "ip:port:idx" (braft::PeerId format)
  std::string initialPeers;  // comma-separated peer id list; only consulted when
                              // bootstrapping a brand-new group (empty log + meta)
  std::filesystem::path dataDir;

  int electionTimeoutMs = 1000;  // specification.md §5.3 default

  // Sized generously so the ring is never expected to apply
  // backpressure in practice (specification.md §5.4); maxInputSize
  // bounds one proposed input's size.
  std::size_t committedEntryRingCapacity = 4096;
  std::size_t maxInputSize = std::size_t{1} << 20;  // 1 MiB

  // -1 = don't pin (the portable default for development and tests).
  // specification.md §5.3 recommends pinning the apply thread to a
  // dedicated core with its hyperthread sibling left empty in
  // production; that is a deployment-time decision this knob exposes
  // rather than one the harness can make on an operator's behalf.
  int applyThreadCpu = -1;
};

class NodeImpl {
 public:
  NodeImpl(NodeConfig config, std::unique_ptr<sequencer::StateMachine> stateMachine);
  ~NodeImpl();

  NodeImpl(const NodeImpl&) = delete;
  NodeImpl& operator=(const NodeImpl&) = delete;

  // Initializes the raft node, starts the brpc server, and starts the
  // pinned apply thread. Does not block. Throws std::runtime_error on
  // any setup failure.
  void start();

  // Stops the apply thread, shuts down the raft node, and stops the
  // brpc server. Safe to call at most once, after start().
  void stop();

  bool isLeader() const;
  int listenPort() const noexcept { return listenPort_; }

 private:
  NodeConfig config_;
  int listenPort_;

  std::unique_ptr<sequencer::StateMachine> stateMachine_;
  std::unique_ptr<journal::JournalWriter> journal_;
  std::unique_ptr<CommittedEntryRing> ring_;
  std::unique_ptr<raft::RaftStateMachineAdapter> adapter_;
  std::unique_ptr<::braft::Node> raftNode_;
  std::unique_ptr<raft::ProposeServiceImpl> proposeService_;
  brpc::Server server_;
  std::unique_ptr<ApplyLoop> applyLoop_;
  std::thread applyThread_;
  std::atomic<bool> stopRequested_{false};
  bool started_ = false;
};

}  // namespace sequencer::node::detail
