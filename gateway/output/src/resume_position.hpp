#pragma once

// The next sequence number to deliver, persisted to a plain file so a
// restarted gateway resumes exactly where it left off
// (specification.md §8.3: "restartable from any sequence number with
// identical output"). Written via write-then-rename for atomicity
// against a crash mid-write; not fsync'd on every record — matching
// the journal's own "asynchronously flushed by default" choice (§5.1)
// rather than paying a sync per delivered record.

#include <cstdint>
#include <system_error>
#include <filesystem>
#include <fstream>

namespace sequencer::gateway::output::detail {

class ResumePosition {
 public:
  explicit ResumePosition(std::filesystem::path path) : path_(std::move(path)) {}

  std::uint64_t load() const {
    std::ifstream file(path_);
    if (!file) {
      return 1;  // no resume file yet: start from the beginning
    }
    std::uint64_t value = 1;
    file >> value;
    return value == 0 ? 1 : value;
  }

  // Never throws.
  //
  // This is a HINT, not a durability guarantee: losing an update costs
  // a restart some replay from an older position, which §8.3 requires
  // to produce identical output anyway. Taking the process down over
  // one is strictly worse than losing it -- and since the write moved
  // onto its own thread, an exception here has nowhere to go and calls
  // std::terminate. That is not hypothetical: the counter FIX
  // end-to-end test removes its temp directory while the gateway is
  // still running, and every run of it crashed the gateway on the way
  // out with "cannot rename: No such file or directory". The tests
  // still passed, because by then they had what they came for, so the
  // crash sat in the log output being ignored.
  //
  // gateway/fix/ and gateway/quickfix/ both guard their own stores for
  // exactly this reason; this one was missed.
  void store(std::uint64_t nextSequenceNumber) noexcept {
    try {
      const std::filesystem::path tmp = path_.string() + ".tmp";
      {
        std::ofstream file(tmp, std::ios::trunc);
        file << nextSequenceNumber;
        file.flush();
        if (!file) {
          return;  // do not rename a short write over a good position
        }
      }
      std::error_code ec;
      std::filesystem::rename(tmp, path_, ec);
    } catch (const std::exception&) {
      // Deliberately swallowed; see above.
    }
  }

 private:
  std::filesystem::path path_;
};

}  // namespace sequencer::gateway::output::detail
