#pragma once

// The next sequence number to deliver, persisted to a plain file so a
// restarted gateway resumes exactly where it left off
// (specification.md §8.3: "restartable from any sequence number with
// identical output"). Written via write-then-rename for atomicity
// against a crash mid-write; not fsync'd on every record — matching
// the journal's own "asynchronously flushed by default" choice (§5.1)
// rather than paying a sync per delivered record.

#include <cstdint>
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

  void store(std::uint64_t nextSequenceNumber) {
    const std::filesystem::path tmp = path_.string() + ".tmp";
    {
      std::ofstream file(tmp, std::ios::trunc);
      file << nextSequenceNumber;
    }
    std::filesystem::rename(tmp, path_);
  }

 private:
  std::filesystem::path path_;
};

}  // namespace sequencer::gateway::output::detail
