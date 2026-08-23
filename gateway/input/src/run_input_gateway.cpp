#include <sequencer/input_gateway.hpp>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <atomic>
#include <chrono>
#include <csignal>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "input_gateway_impl.hpp"

DEFINE_string(node_peers, "",
              "Comma-separated \"ip:port\" Propose endpoints of the raft group's nodes (required)");
DEFINE_int32(listen_port, 0, "This gateway's own client-facing port (required)");

namespace sequencer {
namespace {

std::vector<std::string> splitCommaSeparated(const std::string& s) {
  std::vector<std::string> parts;
  std::stringstream ss(s);
  std::string part;
  while (std::getline(ss, part, ',')) {
    if (!part.empty()) {
      parts.push_back(part);
    }
  }
  return parts;
}

std::atomic<bool> gStopRequested{false};
void handleStopSignal(int /*signum*/) { gStopRequested.store(true, std::memory_order_relaxed); }

}  // namespace

int RunInputGateway(int argc, char** argv, std::unique_ptr<InputCodec> codec) {
  google::InitGoogleLogging(argv[0]);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  if (FLAGS_node_peers.empty() || FLAGS_listen_port == 0) {
    LOG(ERROR) << "RunInputGateway: --node_peers and --listen_port are required";
    return 1;
  }

  gateway::input::detail::InputGatewayConfig config;
  config.nodeEndpoints = splitCommaSeparated(FLAGS_node_peers);
  config.listenPort = FLAGS_listen_port;

  gateway::input::detail::InputGatewayImpl gateway(std::move(config), std::move(codec));
  gateway.start();
  LOG(INFO) << "input gateway started: listen_port=" << FLAGS_listen_port
            << " node_peers=" << FLAGS_node_peers;

  std::signal(SIGINT, handleStopSignal);
  std::signal(SIGTERM, handleStopSignal);
  while (!gStopRequested.load(std::memory_order_relaxed)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }

  LOG(INFO) << "input gateway stopping";
  gateway.stop();
  return 0;
}

}  // namespace sequencer
