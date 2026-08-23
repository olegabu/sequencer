#pragma once

// Bridges braft's callback-driven StateMachine interface to the
// sequencer's own (specification.md §4). on_apply() only copies each
// committed task's bytes into the committed-entry ring and returns
// immediately — it deliberately never calls the user's
// StateMachine::apply() or touches the journal itself. That work
// happens exclusively on the pinned apply thread (apply_loop.hpp), off
// braft's own FSMCaller thread, exactly matching specification.md
// §5.1's diagram.

#include <braft/raft.h>
#include <braft/storage.h>
#include <butil/iobuf.h>
#include <glog/logging.h>

#include <cerrno>
#include <cstddef>
#include <filesystem>
#include <stdexcept>
#include <string>

#include <sequencer/payload.hpp>
#include <sequencer/state_machine.hpp>

#include "../committed_entry_ring.hpp"

namespace sequencer::node::raft {

class RaftStateMachineAdapter : public ::braft::StateMachine {
 public:
  RaftStateMachineAdapter(detail::CommittedEntryRing& ring, sequencer::StateMachine& userStateMachine,
                           std::string snapshotFileName)
      : ring_(ring), userStateMachine_(userStateMachine), snapshotFileName_(std::move(snapshotFileName)) {}

  void on_apply(::braft::Iterator& iter) override {
    for (; iter.valid(); iter.next()) {
      // iter.data() is only valid for the duration of this callback, so
      // it must be copied now — not left as a view for the apply thread
      // to dereference later (the ring's own push() will make a second,
      // ring-owned copy from these bytes; see committed_entry_ring.hpp
      // for why that copy is unavoidable here).
      const std::string bytes = iter.data().to_string();
      const Payload input(reinterpret_cast<const std::byte*>(bytes.data()), bytes.size());

      // Ownership of a non-null done passes to on_apply per braft's
      // Task::done contract; we hand that ownership on again to the
      // apply thread's completion callback (apply_loop.hpp), which
      // calls done->Run() only after the record is durably journaled.
      ring_.push(input, iter.done());
    }
  }

  void on_snapshot_save(::braft::SnapshotWriter* writer, ::braft::Closure* done) override {
    butil::Status status;
    try {
      const std::filesystem::path path = std::filesystem::path(writer->get_path()) / snapshotFileName_;
      {
        sequencer::SnapshotWriter userWriter(path);
        userStateMachine_.snapshotSave(userWriter);
      }
      if (writer->add_file(snapshotFileName_) != 0) {
        status.set_error(EIO, "SnapshotWriter::add_file failed for %s", snapshotFileName_.c_str());
      }
    } catch (const std::exception& e) {
      status.set_error(EIO, "snapshotSave failed: %s", e.what());
    }
    done->status() = status;
    done->Run();
  }

  int on_snapshot_load(::braft::SnapshotReader* reader) override {
    try {
      const std::filesystem::path path = std::filesystem::path(reader->get_path()) / snapshotFileName_;
      sequencer::SnapshotReader userReader(path);
      userStateMachine_.snapshotLoad(userReader);
      return 0;
    } catch (const std::exception& e) {
      LOG(ERROR) << "snapshotLoad failed: " << e.what();
      return EIO;
    }
  }

  void on_leader_start(std::int64_t term) override {
    LOG(INFO) << "this node became leader at term " << term;
  }
  void on_leader_stop(const butil::Status& status) override {
    LOG(INFO) << "this node stepped down as leader: " << status;
  }
  void on_shutdown() override { LOG(INFO) << "state machine shut down"; }
  void on_error(const ::braft::Error& e) override { LOG(ERROR) << "raft error: " << e; }

 private:
  detail::CommittedEntryRing& ring_;
  sequencer::StateMachine& userStateMachine_;
  std::string snapshotFileName_;
};

}  // namespace sequencer::node::raft
