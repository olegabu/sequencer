#include <sequencer/output_gateway.hpp>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <atomic>
#include <chrono>
#include <csignal>
#include <thread>

#include "output_gateway_impl.hpp"

DEFINE_string(data_dir, "", "A node's journal directory to tail, colocated (required)");
DEFINE_string(resume_file, "", "Where to durably persist this gateway's resume position (required)");
DEFINE_int32(listen_port, 0, "This gateway's own client-facing port (required)");

namespace sequencer {
namespace {

std::atomic<bool> gStopRequested{false};
void handleStopSignal(int /*signum*/) { gStopRequested.store(true, std::memory_order_relaxed); }

}  // namespace

int RunOutputGateway(int argc, char** argv, std::unique_ptr<OutputCodec> codec) {
  google::InitGoogleLogging(argv[0]);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  if (FLAGS_data_dir.empty() || FLAGS_resume_file.empty() || FLAGS_listen_port == 0) {
    LOG(ERROR) << "RunOutputGateway: --data_dir, --resume_file, and --listen_port are required";
    return 1;
  }

  gateway::output::detail::OutputGatewayConfig config;
  config.dataDir = FLAGS_data_dir;
  config.resumeFile = FLAGS_resume_file;
  config.listenPort = FLAGS_listen_port;

  gateway::output::detail::OutputGatewayImpl gateway(std::move(config), std::move(codec));
  gateway.start();
  LOG(INFO) << "output gateway started: listen_port=" << FLAGS_listen_port
            << " data_dir=" << FLAGS_data_dir;

  std::signal(SIGINT, handleStopSignal);
  std::signal(SIGTERM, handleStopSignal);
  while (!gStopRequested.load(std::memory_order_relaxed)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }

  LOG(INFO) << "output gateway stopping";
  gateway.stop();
  return 0;
}

}  // namespace sequencer
