#include <sequencer/output_gateway.hpp>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <csignal>
#include <functional>
#include <thread>

#include "brpc_stream_transport.hpp"
#include "output_gateway_impl.hpp"

DEFINE_string(data_dir, "", "A node's journal directory to tail, colocated (required)");
DEFINE_string(resume_file, "", "Where to durably persist this gateway's resume position (required)");
DEFINE_int32(listen_port, 0, "This gateway's own client-facing port (required)");
// See OutputGatewayConfig::batchWindow's own comment (output_gateway_impl.hpp)
// for what this trades off and why it defaults to 0 (off).
DEFINE_int32(batch_window_us, 0,
             "Once at least one record is available, keep gathering for up to this many "
             "microseconds before flushing to the transport, even if nothing new has shown up "
             "yet. 0 (default): never delay, flush whatever's immediately available.");

namespace sequencer {
namespace {

std::atomic<bool> gStopRequested{false};
void handleStopSignal(int /*signum*/) { gStopRequested.store(true, std::memory_order_relaxed); }

int runOutputGatewayCommon(int argc, char** argv, std::unique_ptr<OutputCodec> codec,
                            const std::function<std::unique_ptr<OutputTransport>()>& transportFactory) {
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
  config.batchWindow = std::chrono::microseconds(std::max(0, FLAGS_batch_window_us));

  gateway::output::detail::OutputGatewayImpl gateway(std::move(config), std::move(codec),
                                                       transportFactory());
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

}  // namespace

int RunOutputGateway(int argc, char** argv, std::unique_ptr<OutputCodec> codec) {
  return runOutputGatewayCommon(argc, argv, std::move(codec), [] {
    return std::make_unique<gateway::output::detail::BrpcStreamTransport>();
  });
}

int RunOutputGateway(int argc, char** argv, std::unique_ptr<OutputCodec> codec,
                      std::function<std::unique_ptr<OutputTransport>()> transportFactory) {
  return runOutputGatewayCommon(argc, argv, std::move(codec), transportFactory);
}

}  // namespace sequencer
