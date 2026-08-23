#pragma once

// specification.md §7.4: "treat a proof's non-arrival within a bounded
// interval as a first-class alarm." A minimal tracker: register a
// sequence number when its proof is expected to start being awaited
// (typically right after the receipt for it arrives), clear it when the
// proof actually shows up, and ask which registered sequence numbers
// have gone past the bound without one. Header-only and dependency-free
// — no crypto, no journal, nothing but <chrono> and a map — since
// raising the alarm needs none of that; acting on it (reconstructing
// the proof from the published journal, per §7.4) is the caller's job,
// using ProofVerifier/evidence:: directly.

#include <chrono>
#include <cstdint>
#include <map>
#include <mutex>
#include <vector>

namespace sequencer::sdk {

class ProofTimeoutAlarm {
 public:
  explicit ProofTimeoutAlarm(std::chrono::milliseconds bound) : bound_(bound) {}

  void submitted(std::uint64_t sequenceNumber,
                 std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now()) {
    std::lock_guard<std::mutex> lock(mutex_);
    pending_[sequenceNumber] = now;
  }

  void proofReceived(std::uint64_t sequenceNumber) {
    std::lock_guard<std::mutex> lock(mutex_);
    pending_.erase(sequenceNumber);
  }

  // Every registered sequence number still awaiting a proof past the
  // bound, oldest first — specification.md §7.4's alarm, ready for the
  // caller to escalate and, "if the pattern is systematic," stop
  // submitting.
  std::vector<std::uint64_t> overdue(
      std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now()) const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<std::uint64_t> result;
    for (const auto& [sequenceNumber, submittedAt] : pending_) {
      if (now - submittedAt >= bound_) {
        result.push_back(sequenceNumber);
      }
    }
    return result;
  }

  std::size_t pendingCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return pending_.size();
  }

 private:
  std::chrono::milliseconds bound_;
  mutable std::mutex mutex_;
  std::map<std::uint64_t, std::chrono::steady_clock::time_point> pending_;  // ordered: oldest first
};

}  // namespace sequencer::sdk
