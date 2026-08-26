#pragma once

// The Propose RPC handler (specification.md §3, §5.2): submits the
// request's opaque bytes as a raft task and blocks the calling bthread
// — never the underlying OS thread — until either braft rejects the
// task outright or the pinned apply thread has applied it and durably
// journaled it (§5.1: "journal append precedes acknowledgement").

#include <braft/raft.h>
#include <brpc/closure_guard.h>
#include <butil/iobuf.h>

#include <deque>
#include <vector>

#include "node.pb.h"
#include "propose_closure.hpp"

namespace sequencer::node::raft {

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
    raftNode_.apply(task);
    closure.event.wait();

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

    if (!raftNode_.is_leader()) {
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
  }

 private:
  ::braft::Node& raftNode_;
};

}  // namespace sequencer::node::raft
