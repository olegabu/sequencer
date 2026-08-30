#pragma once

#include <cstdlib>
#include <filesystem>

#include <sequencer/journal/format.hpp>
#include <stdexcept>
#include <string>

namespace sequencer::journal::testing {

// A uniquely-named scratch directory, removed on destruction.
class TempDir {
 public:
  TempDir() {
    std::string tmpl = (std::filesystem::temp_directory_path() / "sequencer_journal_XXXXXX").string();
    if (::mkdtemp(tmpl.data()) == nullptr) {
      throw std::runtime_error("mkdtemp failed");
    }
    path_ = tmpl;
  }

  ~TempDir() {
    std::error_code ec;
    std::filesystem::remove_all(path_, ec);
  }

  TempDir(const TempDir&) = delete;
  TempDir& operator=(const TempDir&) = delete;

  const std::filesystem::path& path() const noexcept { return path_; }
  // A journal is a directory of segments now (§6.5), so these name the
  // FIRST segment's files rather than "the" journal files. Tests that
  // reach past JournalWriter to manipulate bytes directly want segment
  // zero, since a test journal rarely rolls.
  std::filesystem::path manifestPath() const { return path_ / kManifestFileName; }
  std::filesystem::path dataPath() const {
    return path_ / (openSegmentStem(1) + ".data");
  }
  std::filesystem::path indexPath() const {
    return path_ / (openSegmentStem(1) + ".index");
  }

 private:
  std::filesystem::path path_;
};

}  // namespace sequencer::journal::testing
