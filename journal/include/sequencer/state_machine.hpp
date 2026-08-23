#pragma once

// The state machine interface — specification.md §4 (normative). This
// is the entire surface an application implements. It lives here, in
// journal/ (the component with no dependencies of its own —
// specification.md §9), rather than in node/, because more than one
// component needs it without needing node/'s braft/brpc machinery:
// node/ calls apply() from the pinned apply thread, and tools/replay
// calls it too, to replay a recorded journal through a fresh instance,
// with neither braft nor brpc anywhere in its link.

#include <array>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <optional>
#include <span>
#include <stdexcept>

#include <sequencer/payload.hpp>

namespace sequencer {

// Collects the outputs a state machine emits for one apply() call.
// Non-owning: emit() records only the Payload (pointer + length) handed
// to it — the bytes themselves must remain valid until the *next*
// apply() call, exactly like apply()'s own `input` parameter
// (specification.md §4's "a reused arena" note). A fresh instance's
// state is reset by the harness before every apply() call.
class OutputCollector {
 public:
  // A generous, fixed cap so this type never allocates (specification.md
  // §4.1: "allocate unboundedly" is forbidden). A state machine emitting
  // more outputs than this for one input is almost certainly a bug —
  // emit() throws rather than silently dropping an output.
  static constexpr std::size_t kMaxOutputsPerInput = 64;

  void emit(Payload output) {
    if (count_ >= kMaxOutputsPerInput) {
      throw std::length_error("OutputCollector::emit: exceeded kMaxOutputsPerInput");
    }
    outputs_[count_++] = output;
  }

  void designateOutput(std::size_t index) {
    if (index >= count_) {
      throw std::out_of_range("OutputCollector::designateOutput: index >= emitted output count");
    }
    designated_ = index;
  }

  // --- Harness-internal accessors (not part of the application-facing
  // surface in §4, but needed by node/ and tools/replay to retrieve
  // what apply() just collected) ---

  void reset() noexcept {
    count_ = 0;
    designated_.reset();
  }

  std::span<const Payload> outputs() const noexcept { return {outputs_.data(), count_}; }

  Payload designatedOutput() const noexcept {
    return designated_.has_value() ? outputs_[*designated_] : Payload{};
  }

 private:
  std::array<Payload, kMaxOutputsPerInput> outputs_{};
  std::size_t count_ = 0;
  std::optional<std::size_t> designated_;
};

// A sequential byte sink for StateMachine::snapshotSave(), backed by a
// single file. node/src/raft/ opens one of these at a path inside
// braft's own snapshot directory before calling snapshotSave(), and
// registers that file with braft afterward — the state machine itself
// never sees a braft type (specification.md §9: "no braft type appears
// in any public interface").
class SnapshotWriter {
 public:
  explicit SnapshotWriter(const std::filesystem::path& path) : file_(path, std::ios::binary) {
    if (!file_) {
      throw std::runtime_error("SnapshotWriter: failed to open " + path.string());
    }
    file_.exceptions(std::ios::badbit | std::ios::failbit);
  }

  void write(const void* data, std::size_t size) {
    file_.write(static_cast<const char*>(data), static_cast<std::streamsize>(size));
  }

 private:
  std::ofstream file_;
};

// The sequential-read counterpart of SnapshotWriter, for
// StateMachine::snapshotLoad().
class SnapshotReader {
 public:
  explicit SnapshotReader(const std::filesystem::path& path) : file_(path, std::ios::binary) {
    if (!file_) {
      throw std::runtime_error("SnapshotReader: failed to open " + path.string());
    }
    file_.exceptions(std::ios::badbit | std::ios::failbit);
  }

  void read(void* data, std::size_t size) {
    file_.read(static_cast<char*>(data), static_cast<std::streamsize>(size));
  }

 private:
  std::ifstream file_;
};

// specification.md §4: implement this, follow the determinism rules in
// §4.1, and the harness makes your logic replicated, ordered, durable,
// disseminated, and provable.
class StateMachine {
 public:
  virtual ~StateMachine() = default;

  // Called on the single pinned apply thread, once per committed input,
  // in sequence-number order. `input` and every emitted output are
  // valid only until the next call to apply() (a reused arena; the
  // state machine must not retain pointers across calls).
  virtual void apply(std::uint64_t sequenceNumber, Payload input, OutputCollector& outputs) = 0;

  virtual void snapshotSave(SnapshotWriter&) = 0;  // full-state serialize
  virtual void snapshotLoad(SnapshotReader&) = 0;  // exact-state restore
};

}  // namespace sequencer
