#include "node_impl.hpp"

#include <braft/configuration.h>
#include <braft/util.h>
#include <bvar/bvar.h>
#include <glog/logging.h>

#include <pthread.h>
#include <sched.h>

#include <stdexcept>
#include <system_error>

#include "raft/propose_closure.hpp"

namespace sequencer::node::detail {

namespace {
constexpr char kSnapshotFileName[] = "state.bin";
}  // namespace

NodeImpl::NodeImpl(NodeConfig config, std::unique_ptr<sequencer::StateMachine> stateMachine)
    : config_(std::move(config)),
      listenPort_(::braft::PeerId(config_.peerId).addr.port),
      stateMachine_(std::move(stateMachine)) {}

NodeImpl::~NodeImpl() {
  if (started_) {
    stop();
  }
}

void NodeImpl::start() {
  std::filesystem::create_directories(config_.dataDir);
  const std::filesystem::path logPath = config_.dataDir / "log";
  const std::filesystem::path metaPath = config_.dataDir / "raft_meta";
  const std::filesystem::path snapshotPath = config_.dataDir / "snapshot";

  journal_ = std::make_unique<journal::JournalWriter>(config_.dataDir / "journal",
                                                        config_.journalOptions);
  ring_ = std::make_unique<CommittedEntryRing>(config_.committedEntryRingCapacity, config_.maxInputSize);
  adapter_ = std::make_unique<raft::RaftStateMachineAdapter>(*ring_, *stateMachine_, kSnapshotFileName);

  // braft::Node::init() requires an RPC server with braft's replication
  // services already attached and started at this node's own address —
  // it checks for that server on init and fails loudly if it's missing
  // ("did you forget to call braft::add_service()?"). So the brpc
  // server (§3.1: one server per node, wearing three hats — Propose,
  // braft's own replication RPCs, and brpc's monitoring pages) must be
  // fully up before raftNode_->init() runs, even though the Propose
  // service it hosts needs a reference to raftNode_ to be constructed.
  // braft::Node's constructor alone (no init() yet) is enough for that
  // reference to be valid.
  raftNode_ = std::make_unique<::braft::Node>(config_.groupId, ::braft::PeerId(config_.peerId));

  proposeService_ = std::make_unique<raft::ProposeServiceImpl>(*raftNode_);
  if (server_.AddService(proposeService_.get(), brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
    throw std::runtime_error("NodeImpl::start: brpc::Server::AddService(ProposeService) failed");
  }
  if (::braft::add_service(&server_, listenPort_) != 0) {
    throw std::runtime_error("NodeImpl::start: braft::add_service failed");
  }

  brpc::ServerOptions serverOptions;
  if (server_.Start(listenPort_, &serverOptions) != 0) {
    throw std::runtime_error("NodeImpl::start: brpc::Server::Start failed on port " +
                              std::to_string(listenPort_));
  }

  ::braft::NodeOptions options;
  options.election_timeout_ms = config_.electionTimeoutMs;
  options.fsm = adapter_.get();
  options.node_owns_fsm = false;
  options.log_uri = "local://" + logPath.string();
  options.raft_meta_uri = "local://" + metaPath.string();
  options.snapshot_uri = "local://" + snapshotPath.string();
  options.disable_cli = false;
  if (!config_.initialPeers.empty()) {
    if (options.initial_conf.parse_from(config_.initialPeers) != 0) {
      throw std::runtime_error("NodeImpl::start: failed to parse initialPeers: " + config_.initialPeers);
    }
  }

  if (raftNode_->init(options) != 0) {
    throw std::runtime_error("NodeImpl::start: braft::Node::init failed for peer " + config_.peerId);
  }

  applyLoop_ = std::make_unique<ApplyLoop>(*stateMachine_, *journal_, *ring_, &raft::onEntryApplied);
  applyThread_ = std::thread([this] {
    if (config_.applyThreadCpu >= 0) {
      cpu_set_t cpuSet;
      CPU_ZERO(&cpuSet);
      CPU_SET(config_.applyThreadCpu, &cpuSet);
      const int rc = pthread_setaffinity_np(pthread_self(), sizeof(cpuSet), &cpuSet);
      if (rc != 0) {
        LOG(WARNING) << "failed to pin apply thread to CPU " << config_.applyThreadCpu << ": "
                      << std::system_category().message(rc);
      }
    }
    // specification.md §5.4: the apply thread busy-spins unconditionally.
    applyLoop_->run(stopRequested_, config_.applyThreadPureSpin);

    // run() returns either because stop was requested, or because the
    // journal filled. The second case used to be a std::terminate with
    // one cryptic line of output; say plainly what happened and what to
    // do about it. The node stays up -- it can still vote and serve
    // reads of what it already has -- but it applies nothing further,
    // so raft should be allowed to lead elsewhere.
    if (applyLoop_->halted()) {
      LOG(ERROR) << "apply thread stopped: " << applyLoop_->haltReason();
      LOG(ERROR) << "this node can no longer apply entries; it holds "
                  << journal_->committedCount() << " records. Restart it against a fresh "
                  << "data_dir, or with a larger --journal_max_index_entries / "
                  << "--journal_max_data_bytes (both are sparse reservations).";
    }
  });

  // Published on the node's own /vars page so a long run can watch the
  // journal approach its limit instead of discovering it. Reports
  // whichever of the two reservations is closer to full; see
  // JournalWriter::fillPercent, which is the one member of that class
  // safe to read from a thread other than the apply thread.
  journalFillPercent_ = std::make_unique<bvar::PassiveStatus<int>>(
      "journal_fill_percent",
      [](void* self) {
        auto* impl = static_cast<NodeImpl*>(self);
        return impl->journal_ == nullptr ? 0 : impl->journal_->fillPercent();
      },
      this);

  started_ = true;
}

void NodeImpl::stop() {
  stopRequested_.store(true, std::memory_order_relaxed);
  if (applyThread_.joinable()) {
    applyThread_.join();
  }
  if (raftNode_) {
    raftNode_->shutdown(nullptr);
    raftNode_->join();
  }
  server_.Stop(0);
  server_.Join();
  started_ = false;
}

bool NodeImpl::isLeader() const { return raftNode_ && raftNode_->is_leader(); }

}  // namespace sequencer::node::detail
