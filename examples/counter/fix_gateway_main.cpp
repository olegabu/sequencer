// specification.md §10, §8.12: the counter example's FIX session
// gateway -- order entry and execution reports on one FIX session,
// sharing one session core.
//
// A client sends U1 (tag 5001 = a signed delta) and receives U2 (the
// new total) FROM THE JOURNAL, never as the synchronous reply, per
// §8.11. Subscribe to the totals with a MarketDataRequest naming
// Symbol TOTALS; see counter_fix_codecs.hpp for why the counter
// broadcasts rather than addressing the submitter.
//
//   counter_fix_gateway --node_peers=127.0.0.1:8100 --listen_port=8500
//       --data_dir=/tmp/counter-node-0
//       --resume_file=/tmp/counter-node-0/fix-resume
//       --sequence_store_dir=/tmp/counter-node-0/fix-seq

#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <sequencer/fix/fix_session_gateway.hpp>

#include "counter_fix_codecs.hpp"

DEFINE_string(node_peers, "",
              "Comma-separated raft group Propose endpoints, e.g. 127.0.0.1:8100 (required)");
DEFINE_int32(listen_port, 0, "The FIX port clients connect to (required)");
DEFINE_string(data_dir, "", "A node's journal directory to tail, colocated (required)");
DEFINE_string(resume_file, "", "Where this gateway's journal resume position is kept (required)");
DEFINE_string(sequence_store_dir, "",
              "Where per-session FIX sequence counters are persisted; empty keeps them in memory, "
              "which loses a session's numbers across a restart");
DEFINE_string(sender_comp_id, "SEQUENCER", "This gateway's own FIX CompID");
DEFINE_int32(heartbeat_interval, 30, "FIX HeartBtInt, in seconds");

namespace {

std::vector<std::string> splitCommaSeparated(const std::string& value) {
  std::vector<std::string> parts;
  std::stringstream stream(value);
  std::string part;
  while (std::getline(stream, part, ',')) {
    if (!part.empty()) {
      parts.push_back(part);
    }
  }
  return parts;
}

}  // namespace

int main(int argc, char** argv) {
  google::InitGoogleLogging(argv[0]);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  if (FLAGS_node_peers.empty() || FLAGS_listen_port == 0 || FLAGS_data_dir.empty() ||
      FLAGS_resume_file.empty()) {
    LOG(ERROR) << "counter_fix_gateway: --node_peers, --listen_port, --data_dir and "
                  "--resume_file are required";
    return 1;
  }

  sequencer::fix::SessionGatewayConfig config;
  config.nodeEndpoints = splitCommaSeparated(FLAGS_node_peers);
  config.listenPort = FLAGS_listen_port;
  config.dataDir = FLAGS_data_dir;
  config.resumeFile = FLAGS_resume_file;
  config.senderCompId = FLAGS_sender_comp_id;
  config.heartBtInt = FLAGS_heartbeat_interval;
  config.sequenceStoreDir = FLAGS_sequence_store_dir;

  LOG(INFO) << "counter FIX session gateway starting: listen_port=" << FLAGS_listen_port
            << " node_peers=" << FLAGS_node_peers;

  return sequencer::fix::RunFixSessionGateway(
      std::move(config),
      std::make_unique<sequencer::examples::counter::CounterFixInputCodec>(),
      std::make_unique<sequencer::examples::counter::CounterFixOutputCodec>());
}
