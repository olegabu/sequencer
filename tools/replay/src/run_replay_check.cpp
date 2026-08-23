#include <sequencer/replay.hpp>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <iostream>

#include "replay_check.hpp"

DEFINE_string(data_dir, "", "Directory containing the recorded journal.data/journal.index to replay (required)");
DEFINE_string(replay_output_dir, "",
              "Where to write the replayed journal; if empty, a temp directory is used and removed "
              "on success (left in place for inspection if replay diverges)");

namespace sequencer {

int RunReplayCheck(int argc, char** argv, std::unique_ptr<StateMachine> stateMachine) {
  google::InitGoogleLogging(argv[0]);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  if (FLAGS_data_dir.empty()) {
    LOG(ERROR) << "RunReplayCheck: --data_dir is required";
    return 1;
  }

  replay::detail::ReplayConfig config;
  config.dataDir = FLAGS_data_dir;
  config.replayOutputDir = FLAGS_replay_output_dir;

  try {
    const replay::detail::ReplayResult result = replay::detail::runReplayCheck(config, *stateMachine);
    if (result.ok) {
      std::cout << result.message << std::endl;
      return 0;
    }
    std::cerr << result.message << std::endl;
    std::cerr << "replayed journal left at: " << result.replayOutputDir.string() << std::endl;
    return 1;
  } catch (const std::exception& e) {
    std::cerr << "replay failed: " << e.what() << std::endl;
    return 1;
  }
}

}  // namespace sequencer
