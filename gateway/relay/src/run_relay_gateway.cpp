// The relay gateway's standalone binary (specification.md §8.2, §9).
// Unlike RunInputGateway/RunOutputGateway, a relay needs no
// application codec — it carries a colocated replica's journal off its
// machine unmodified, nothing interpreted, translated, filtered, or
// reordered — so, like evidence/'s signing gateway, it ships as a
// ready-to-run stock binary rather than a library an application links.
// No application repository ever implements one; every deployment
// simply runs this binary, exactly as it would run `dumper`.

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <atomic>
#include <chrono>
#include <csignal>
#include <thread>

#include "relay_gateway_impl.hpp"
#include "relay_server.hpp"

DEFINE_string(data_dir, "", "A node's journal directory to relay, colocated (required)");
DEFINE_int32(listen_port, 0, "This relay's own client-facing port (required)");

namespace sequencer::gateway::relay {
namespace {

std::atomic<bool> gStopRequested{false};
void handleStopSignal(int /*signum*/) { gStopRequested.store(true, std::memory_order_relaxed); }

}  // namespace
}  // namespace sequencer::gateway::relay

int main(int argc, char** argv) {
  google::InitGoogleLogging(argv[0]);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  if (FLAGS_data_dir.empty() || FLAGS_listen_port == 0) {
    LOG(ERROR) << "RunRelayGateway: --data_dir and --listen_port are required";
    return 1;
  }

  sequencer::gateway::relay::detail::RelayGatewayConfig config;
  config.dataDir = FLAGS_data_dir;
  config.listenPort = FLAGS_listen_port;

  sequencer::gateway::relay::detail::RelayGatewayImpl gateway(std::move(config));
  gateway.start();
  sequencer::gateway::relay::detail::RelayServer server(gateway, FLAGS_listen_port);
  LOG(INFO) << "relay gateway started: listen_port=" << FLAGS_listen_port << " data_dir=" << FLAGS_data_dir;

  std::signal(SIGINT, sequencer::gateway::relay::handleStopSignal);
  std::signal(SIGTERM, sequencer::gateway::relay::handleStopSignal);
  while (!sequencer::gateway::relay::gStopRequested.load(std::memory_order_relaxed)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }

  LOG(INFO) << "relay gateway stopping";
  server.stop();
  gateway.stop();
  return 0;
}
