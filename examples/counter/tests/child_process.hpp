#pragma once

// A subprocess running a real compiled binary — used by tests that
// need to exercise an actual deployment shape (three real processes
// talking over real sockets) rather than an in-process stand-in. RAII:
// SIGTERM on destruction, exactly how the acceptance drills in
// specification.md §14 describe stopping a node.

#include <signal.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <stdexcept>
#include <string>
#include <vector>

namespace sequencer::examples::counter {

class ChildProcess {
 public:
  ChildProcess(std::string path, std::vector<std::string> args)
      : path_(std::move(path)), args_(std::move(args)) {
    std::vector<char*> argv;
    argv.push_back(path_.data());
    for (auto& arg : args_) {
      argv.push_back(arg.data());
    }
    argv.push_back(nullptr);

    pid_ = ::fork();
    if (pid_ < 0) {
      throw std::runtime_error("ChildProcess: fork failed");
    }
    if (pid_ == 0) {
      ::execv(path_.c_str(), argv.data());
      ::_exit(127);  // execv only returns on failure
    }
  }

  ChildProcess(const ChildProcess&) = delete;
  ChildProcess& operator=(const ChildProcess&) = delete;

  ~ChildProcess() {
    if (pid_ > 0) {
      ::kill(pid_, SIGTERM);
      int status = 0;
      ::waitpid(pid_, &status, 0);
    }
  }

 private:
  std::string path_;
  std::vector<std::string> args_;
  pid_t pid_ = -1;
};

}  // namespace sequencer::examples::counter
