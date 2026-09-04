// counter_quickfix_gateway — the counter over FIX, on QuickFIX's
// session layer (specification.md §8.13).
//
// The same codecs, the same journal and the same §8.11 delivery as
// counter_fix_gateway; only the session layer differs. Both binaries
// are built and both are supported: gateway/quickfix/README.md says
// when each is the right choice.

#include "counter_fix_codecs.hpp"

#include <sequencer/comma_separated.hpp>
#include <sequencer/quickfix/quickfix_session_gateway.hpp>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <memory>
#include <sstream>
#include <string>
#include <vector>

DEFINE_string(node_peers, "", "Comma-separated ip:port of the raft group's Propose endpoints");
DEFINE_int32(listen_port, 0, "The FIX port clients connect to (required)");
DEFINE_string(data_dir, "", "A node's journal directory to tail, colocated (required)");
DEFINE_string(resume_file, "", "Where this gateway's journal resume position is kept (required)");
DEFINE_string(sequence_store_dir, "",
              "Where per-session FIX sequence numbers are persisted; empty keeps them in memory");
DEFINE_string(sender_comp_id, "SEQUENCER", "This gateway's own FIX CompID");
DEFINE_int32(heartbeat_interval, 30, "FIX HeartBtInt, in seconds");
DEFINE_string(client_comp_ids, "",
              "Comma-separated CompIDs of every client that may connect. REQUIRED, and the one "
              "configuration counter_fix_gateway does not need: QuickFIX 1.15.1 declares its "
              "acceptor sessions up front and has no dynamic ones, so a counterparty absent from "
              "this list cannot log on. The hffix gateway adopts identity from the Logon instead");

namespace {


}  // namespace

int main(int argc, char** argv) {
  google::InitGoogleLogging(argv[0]);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  if (FLAGS_node_peers.empty() || FLAGS_listen_port == 0 || FLAGS_data_dir.empty() ||
      FLAGS_resume_file.empty()) {
    LOG(ERROR) << "counter_quickfix_gateway: --node_peers, --listen_port, --data_dir and "
                  "--resume_file are required";
    return 1;
  }
  if (FLAGS_client_comp_ids.empty()) {
    LOG(ERROR) << "counter_quickfix_gateway: --client_comp_ids is required -- QuickFIX needs its "
                  "acceptor sessions declared before any client connects";
    return 1;
  }

  sequencer::quickfix::QuickFixGatewayConfig config;
  config.nodeEndpoints = sequencer::splitCommaSeparated(FLAGS_node_peers);
  config.listenPort = FLAGS_listen_port;
  config.dataDir = FLAGS_data_dir;
  config.resumeFile = FLAGS_resume_file;
  config.senderCompId = FLAGS_sender_comp_id;
  config.heartBtInt = FLAGS_heartbeat_interval;
  config.sequenceStoreDir = FLAGS_sequence_store_dir;
  config.clientCompIds = sequencer::splitCommaSeparated(FLAGS_client_comp_ids);

  LOG(INFO) << "counter QuickFIX session gateway starting: listen_port=" << FLAGS_listen_port
            << " clients=" << FLAGS_client_comp_ids;

  return sequencer::quickfix::RunQuickFixSessionGateway(
      std::move(config),
      std::make_unique<sequencer::examples::counter::CounterFixInputCodec>(),
      std::make_unique<sequencer::examples::counter::CounterFixOutputCodec>());
}
