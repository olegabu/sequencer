#pragma once

// A private temp directory, which nineteen files were each defining for
// themselves. The copies differed only in the prefix baked into the
// template, in whether the failure path threw or called std::abort(),
// and in whether the return statement said `tmpl` or
// `std::filesystem::path(tmpl)`.
//
// Mostly used by tests and benchmarks, but not exclusively -- the
// replay-check tool needs one too, which is why this sits in
// sequencer::common rather than behind a test-only target.

#include <cstdlib>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <string_view>

namespace sequencer {

// mkdtemp, not a name built from pid and clock: it creates the
// directory atomically with 0700, so two processes cannot be handed the
// same path. One caller did build its name by hand from getpid() and a
// steady_clock reading and then called create_directories() on it,
// which is both racy and able to succeed on a directory that already
// exists; it uses this now.
inline std::filesystem::path makeTempDir(std::string_view prefix) {
  std::string tmpl =
      (std::filesystem::temp_directory_path() / (std::string(prefix) + "_XXXXXX")).string();
  if (::mkdtemp(tmpl.data()) == nullptr) {
    throw std::runtime_error("mkdtemp failed for prefix: " + std::string(prefix));
  }
  return std::filesystem::path(tmpl);
}

}  // namespace sequencer
