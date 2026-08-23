#pragma once

// The Propose RPC handler (specification.md §3, §5.2): submits the
// request's opaque bytes as a raft task and blocks the calling bthread
// — never the underlying OS thread — until either braft rejects the
// task outright or the pinned apply thread has applied it and durably
// journaled it (§5.1: "journal append precedes acknowledgement").

#include <braft/raft.h>
#include <brpc/closure_guard.h>
#include <butil/iobuf.h>

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

 private:
  ::braft::Node& raftNode_;
};

}  // namespace sequencer::node::raft
