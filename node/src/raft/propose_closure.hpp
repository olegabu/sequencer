#pragma once

// The bridge between one Propose RPC and the two ways it can complete
// (specification.md §5.2): braft rejects the task outright (e.g. a
// leadership change before commit — status() already carries the
// error when Run() is invoked directly by braft), or it commits and is
// later applied and journaled on the pinned apply thread, which calls
// Run() itself via onEntryApplied() below once the record is durable
// (§5.1: "journal append precedes acknowledgement").
//
// bthread::CountdownEvent, not a plain condition variable: the Propose
// RPC handler runs on a bthread (brpc's userspace coroutine), and
// wait()/signal() park and wake the bthread without blocking the OS
// thread underneath it — exactly the primitive braft's own examples use
// for this pattern.

#include <braft/raft.h>
#include <bthread/countdown_event.h>

#include <cstdint>
#include <string>

#include <sequencer/payload.hpp>

#include "../apply_loop.hpp"

namespace sequencer::node::raft {

class ProposeClosure : public ::braft::Closure {
 public:
  void Run() override { event.signal(); }

  bthread::CountdownEvent event{1};
  std::uint64_t sequenceNumber = 0;
  std::string designatedOutput;
};

// apply_loop.hpp's CompletionCallback, bound to a ProposeClosure. Safe
// to `static_cast` rather than `dynamic_cast`: every task this harness
// submits to braft::Node::apply() attaches exactly a ProposeClosure
// (Propose is the only task producer node/ has today) — see node.proto
// and node_impl.cpp.
inline void onEntryApplied(void* context, std::uint64_t sequenceNumber, Payload designatedOutput) {
  if (context == nullptr) {
    return;
  }
  auto* closure = static_cast<ProposeClosure*>(context);
  closure->sequenceNumber = sequenceNumber;
  closure->designatedOutput.assign(reinterpret_cast<const char*>(designatedOutput.data()),
                                    designatedOutput.size());
  closure->Run();
}

}  // namespace sequencer::node::raft
