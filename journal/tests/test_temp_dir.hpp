#pragma once

#include <cstdlib>
#include <filesystem>
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
  std::filesystem::path dataPath() const { return path_ / "journal.data"; }
  std::filesystem::path indexPath() const { return path_ / "journal.index"; }

 private:
  std::filesystem::path path_;
};

}  // namespace sequencer::journal::testing
