#include <sequencer/input_gateway.hpp>

#include <sequencer/stop_signal.hpp>
#include <sequencer/comma_separated.hpp>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <algorithm>
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
// See gateway/input/src/node_proposer.hpp's proposeAsync comment.
DEFINE_int32(max_batch_size, 0,
             "Largest number of client proposals sent to the node in one ProposeBatch; "
             "0 keeps the built-in default");
DEFINE_int32(max_inflight_batches, 0,
             "How many ProposeBatch RPCs may be outstanding at once; 0 keeps the built-in default");

namespace sequencer {
namespace {



}  // namespace

int RunInputGateway(int argc, char** argv, std::unique_ptr<InputCodec> codec) {
  return RunInputGateway(argc, argv, std::move(codec),
                          gateway::input::detail::defaultInputTransportFactory());
}

int RunInputGateway(int argc, char** argv, std::unique_ptr<InputCodec> codec,
                     std::function<std::unique_ptr<InputTransport>()> transportFactory) {
  google::InitGoogleLogging(argv[0]);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  if (FLAGS_node_peers.empty() || FLAGS_listen_port == 0) {
    LOG(ERROR) << "RunInputGateway: --node_peers and --listen_port are required";
    return 1;
  }

  gateway::input::detail::InputGatewayConfig config;
  config.nodeEndpoints = sequencer::splitCommaSeparated(FLAGS_node_peers);
  config.listenPort = FLAGS_listen_port;
  config.maxBatchSize = static_cast<std::size_t>(std::max(0, FLAGS_max_batch_size));
  config.maxInFlightBatches = std::max(0, FLAGS_max_inflight_batches);

  gateway::input::detail::InputGatewayImpl gateway(std::move(config), std::move(codec),
                                                    sequencer::acceptAllSignatures,
                                                    std::move(transportFactory));
  gateway.start();
  LOG(INFO) << "input gateway started: listen_port=" << FLAGS_listen_port
            << " node_peers=" << FLAGS_node_peers;

  sequencer::waitForStopSignal();

  LOG(INFO) << "input gateway stopping";
  gateway.stop();
  return 0;
}

}  // namespace sequencer
