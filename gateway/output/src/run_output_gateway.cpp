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
// See OutputGatewayConfig's own comments (output_gateway_impl.hpp) for
// each of these. (--batch_window_us is gone: an artifact of the old
// push-through-per-session-queues design, obsoleted by reader-side
// draining — see examples/counter/README.md's benchmark section.)
DEFINE_int32(ring_slots, 65536,
             "BroadcastRing capacity in slots (power of two). Together with --ring_max_payload "
             "this bounds how far a slow subscriber can lag before being disconnected.");
DEFINE_int32(ring_max_payload, 512,
             "Largest codec output (bytes) the ring can carry; larger publishes throw.");
DEFINE_int32(idle_spin_iterations, 1000,
             "How long the tailing thread and each subscriber's reader busy-spin before backing "
             "off when caught up.");

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
  config.ringSlots = static_cast<std::size_t>(std::max(2, FLAGS_ring_slots));
  config.ringMaxPayload = static_cast<std::size_t>(std::max(8, FLAGS_ring_max_payload));
  config.idleSpinIterations = std::max(0, FLAGS_idle_spin_iterations);

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
