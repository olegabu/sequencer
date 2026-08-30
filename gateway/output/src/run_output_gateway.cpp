#include <sequencer/output_gateway.hpp>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <csignal>
#include <functional>
#include <string>
#include <thread>
#include <vector>

#include <sequencer/brpc_output_transport.hpp>
#include "output_gateway_impl.hpp"

DEFINE_string(data_dir, "", "A node's journal directory to tail, colocated (required)");
DEFINE_string(resume_file, "", "Where to durably persist this gateway's resume position (required)");
// No --listen_port here on purpose. A port belongs to a transport, not
// to the chassis, and this chassis serves a list of them. It also
// cannot own that flag name: gateway/input/'s own chassis already
// defines --listen_port, and gflags flags are process-global symbols,
// so any binary linking both libraries failed to link at all
// ("multiple definition of fLI::FLAGS_listen_port"). Each application
// main defines whatever port flags it wants and passes the values in.
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
                            const std::function<std::vector<OutputTransportBinding>()>& bindingsFactory) {
  google::InitGoogleLogging(argv[0]);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  if (FLAGS_data_dir.empty() || FLAGS_resume_file.empty()) {
    LOG(ERROR) << "RunOutputGateway: --data_dir and --resume_file are required";
    return 1;
  }
  gateway::output::detail::OutputGatewayConfig config;
  config.dataDir = FLAGS_data_dir;
  config.resumeFile = FLAGS_resume_file;
  config.ringSlots = static_cast<std::size_t>(std::max(2, FLAGS_ring_slots));
  config.ringMaxPayload = static_cast<std::size_t>(std::max(8, FLAGS_ring_max_payload));
  config.idleSpinIterations = std::max(0, FLAGS_idle_spin_iterations);

  // Built after flag parsing on purpose: an application's own port
  // flags are what decide which transports exist at all.
  std::vector<OutputTransportBinding> requested = bindingsFactory();
  if (requested.empty()) {
    LOG(ERROR) << "RunOutputGateway: no transport enabled — set at least one transport's port";
    return 1;
  }
  std::vector<gateway::output::detail::OutputGatewayImpl::Binding> bindings;
  std::string ports;
  for (OutputTransportBinding& b : requested) {
    if (b.listenPort == 0) {
      LOG(ERROR) << "RunOutputGateway: a transport was registered with no port";
      return 1;
    }
    bindings.push_back({b.factory(), b.listenPort});
    ports += (ports.empty() ? "" : ",") + std::to_string(b.listenPort);
  }

  gateway::output::detail::OutputGatewayImpl gateway(std::move(config), std::move(codec),
                                                       std::move(bindings));
  gateway.start();
  LOG(INFO) << "output gateway started: listen_ports=" << ports
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

int RunOutputGateway(int argc, char** argv, std::unique_ptr<OutputCodec> codec,
                      std::function<std::vector<OutputTransportBinding>()> bindingsFactory) {
  return runOutputGatewayCommon(argc, argv, std::move(codec), bindingsFactory);
}

}  // namespace sequencer
