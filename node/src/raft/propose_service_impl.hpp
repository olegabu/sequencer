#pragma once

// The Propose RPC handler (specification.md §3, §5.2): submits the
// request's opaque bytes as a raft task and blocks the calling bthread
// — never the underlying OS thread — until either braft rejects the
// task outright or the pinned apply thread has applied it and durably
// journaled it (§5.1: "journal append precedes acknowledgement").

#include <braft/raft.h>
#include <brpc/closure_guard.h>
#include <butil/iobuf.h>
#include <butil/time.h>
#include <bvar/bvar.h>

#include <atomic>
#include <cstdint>
#include <deque>
#include <vector>

#include "node.pb.h"
#include "propose_closure.hpp"

namespace sequencer::node::raft {

// The leader-side half of the instrumentation described in
// gateway/input/src/node_proposer.hpp. Sweeps put a throughput ceiling
// near 380k req/s that is not CPU anywhere -- this leader measured
// 38.7% busy at 350k on 16 cores -- so the question is what a proposal
// spends its time waiting for once it arrives.
//
// apply_wait_us is the interesting one: it starts after every task in
// the batch has been handed to braft and ends when the last of them has
// been applied and journaled. It therefore contains replication,
// journal append and apply-thread scheduling, and excludes anything the
// gateway or the network did on the way in. Read alongside the
// gateway's input_gateway_batch_queue_delay_us, the two split a
// request's life into "waiting for a slot at the gateway" and "waiting
// for the raft group", which end-to-end latency alone cannot separate.
inline std::atomic<int>& batchesInProgressGauge() {
  static std::atomic<int> value{0};
  return value;
}

struct ProposeMetrics {
  static ProposeMetrics& instance() {
    static ProposeMetrics metrics;
    return metrics;
  }

  // Whole-batch: apply() returned for every task, until the last one
  // finished. Not per-input, because the batch is applied as a unit and
  // its members overlap.
  bvar::LatencyRecorder applyWaitUs{"node_propose_batch_apply_wait_us"};
  // How many inputs arrive per batch, as the leader sees them. The
  // gateway caps its own at 64; comparing the two says whether batches
  // are arriving full. Windowed for the same reason as the gateway's
  // own batch-size bvar -- a bare IntRecorder publishes a lifetime
  // average, which under a rising sweep reports the history rather than
  // the rate being measured.
  bvar::IntRecorder batchInputsRaw;
  bvar::Window<bvar::IntRecorder> batchInputs{"node_propose_batch_inputs", &batchInputsRaw, -1};
  // Single-input Propose, kept separate so the batched and unbatched
  // paths never average together.
  bvar::LatencyRecorder singleApplyWaitUs{"node_propose_apply_wait_us"};
  bvar::Adder<std::uint64_t> redirects{"node_propose_redirects"};
  bvar::PassiveStatus<int> batchesInProgress{"node_propose_batches_in_progress",
                                              readBatchesInProgress, nullptr};

 private:
  static int readBatchesInProgress(void*) {
    return batchesInProgressGauge().load(std::memory_order_relaxed);
  }
};

// Keeps node_propose_batches_in_progress correct on every exit path,
// including the early returns for a non-leader.
class BatchInProgressScope {
 public:
  BatchInProgressScope() { batchesInProgressGauge().fetch_add(1, std::memory_order_relaxed); }
  ~BatchInProgressScope() { batchesInProgressGauge().fetch_sub(1, std::memory_order_relaxed); }
  BatchInProgressScope(const BatchInProgressScope&) = delete;
  BatchInProgressScope& operator=(const BatchInProgressScope&) = delete;
};


class ProposeServiceImpl : public sequencer::node::proto::ProposeService {
 public:
  explicit ProposeServiceImpl(::braft::Node& raftNode) : raftNode_(raftNode) {}

  void Propose(::google::protobuf::RpcController* /*controller*/,
               const sequencer::node::proto::ProposeRequest* request,
               sequencer::node::proto::ProposeResponse* response,
               ::google::protobuf::Closure* done) override {
    brpc::ClosureGuard doneGuard(done);

    // specification.md §3: "Propose(bytes) -> {...} | redirect(leader) | error".
    if (!raftNode_.is_leader()) {
      ProposeMetrics::instance().redirects << 1;
      response->set_redirect(true);
      const ::braft::PeerId leader = raftNode_.leader_id();
      if (!leader.is_empty()) {
        response->set_leader_hint(leader.to_string());
      }
      return;
    }

    ProposeClosure closure;
    butil::IOBuf data;
    data.append(request->input());

    ::braft::Task task;
    task.data = &data;
    task.done = &closure;
    const std::int64_t appliedAtUs = butil::cpuwide_time_us();
    raftNode_.apply(task);
    closure.event.wait();
    ProposeMetrics::instance().singleApplyWaitUs << (butil::cpuwide_time_us() - appliedAtUs);

    if (!closure.status().ok()) {
      response->set_error_message(closure.status().error_cstr());
      return;
    }

    response->set_sequence_number(closure.sequenceNumber);
    if (!closure.designatedOutput.empty()) {
      response->set_designated_output(closure.designatedOutput);
    }
  }

  // The batched form (node.proto's ProposeBatchRequest). Every input
  // is applied as its own raft task and gets its own sequence number
  // — identical semantics to calling Propose once per input, which is
  // what makes this safe to use as a drop-in by the input gateway.
  //
  // The ordering here is what makes it a win: apply() every task
  // FIRST, then wait for all of them. Applying and waiting one at a
  // time would serialize the batch into N sequential commits and be
  // strictly worse than N separate RPCs. braft's own pipelining then
  // sees the whole batch at once, which is the same reason
  // LEADER_BATCH exists.
  void ProposeBatch(::google::protobuf::RpcController* /*controller*/,
                     const sequencer::node::proto::ProposeBatchRequest* request,
                     sequencer::node::proto::ProposeBatchResponse* response,
                     ::google::protobuf::Closure* done) override {
    brpc::ClosureGuard doneGuard(done);
    const BatchInProgressScope inProgress;

    if (!raftNode_.is_leader()) {
      ProposeMetrics::instance().redirects << 1;
      // Nothing was applied, so the caller resends the whole batch —
      // no partial state to reconcile.
      response->set_redirect(true);
      const ::braft::PeerId leader = raftNode_.leader_id();
      if (!leader.is_empty()) {
        response->set_leader_hint(leader.to_string());
      }
      return;
    }

    const int count = request->inputs_size();
    ProposeMetrics::instance().batchInputsRaw << count;
    // deque, not vector: ProposeClosure holds a CountdownEvent and is
    // neither copyable nor movable, and braft is handed a pointer to
    // each one, so the storage must never reallocate.
    std::deque<ProposeClosure> closures(static_cast<std::size_t>(count));
    std::deque<butil::IOBuf> datas(static_cast<std::size_t>(count));

    for (int i = 0; i < count; ++i) {
      datas[i].append(request->inputs(i));
      ::braft::Task task;
      task.data = &datas[i];
      task.done = &closures[i];
      raftNode_.apply(task);
    }

    // Timed from here, once every task is with braft, so this measures
    // what the raft group does with the batch and not the cost of
    // handing it over.
    const std::int64_t appliedAtUs = butil::cpuwide_time_us();
    for (int i = 0; i < count; ++i) {
      closures[i].event.wait();
      sequencer::node::proto::ProposeResult* result = response->add_results();
      if (!closures[i].status().ok()) {
        result->set_error_message(closures[i].status().error_cstr());
        continue;
      }
      result->set_sequence_number(closures[i].sequenceNumber);
      if (!closures[i].designatedOutput.empty()) {
        result->set_designated_output(closures[i].designatedOutput);
      }
    }
    ProposeMetrics::instance().applyWaitUs << (butil::cpuwide_time_us() - appliedAtUs);
  }

 private:
  ::braft::Node& raftNode_;
};

}  // namespace sequencer::node::raft
