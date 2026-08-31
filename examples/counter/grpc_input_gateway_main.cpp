// specification.md §10: the counter example's real-gRPC submission
// path — see proto/counter_input_grpc.proto's file comment for why
// this exists as its own small service rather than a retrofit of the
// generic, byte-opaque RunInputGateway chassis. Reuses
// gateway/input's own NodeProposer (the exact leader-following retry
// logic input_gateway_main.cpp's chassis already uses internally) so
// this doesn't reimplement redirect-following from scratch.
//
// It reuses that proposer the same WAY the chassis does, too, which is
// the part that matters for throughput: proposeAsync(), on gRPC's
// callback API, rather than the blocking propose().
//
// This file used to call propose() from gRPC's synchronous Service,
// and that is a materially different machine. A blocking handler holds
// one server thread for an entire cross-AZ round trip, so the
// gateway's capacity becomes (threads / RTT) rather than anything
// about the raft group, and every request goes to the leader as its
// own Propose RPC with no opportunity to batch. Measured on the
// chassis, those two properties were worth roughly 1320us -> 891us
// (async alone, by removing the thread ceiling) and then a further
// ~20% at 100k from batching. See gateway/README.md's "Admission
// control" section, and note the corollary documented there: the
// in-flight bound is PER GATEWAY, so a deployment running several of
// these against one leader wants it divided down.
//
// The typed-vs-opaque tradeoff against input_gateway_main.cpp is
// unchanged and is the actual reason to pick between them (§8.10);
// this only removes a performance difference that was never part of
// that tradeoff.

#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "counter_input_grpc.grpc.pb.h"
#include "node_proposer.hpp"

#include <cstddef>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

DEFINE_string(node_peers, "", "Comma-separated raft group Propose endpoints, e.g. 127.0.0.1:8100 (required)");
DEFINE_int32(listen_port, 0, "This gRPC gateway's own client-facing port (required)");
// Same two knobs, same defaults and same meaning as the chassis's
// (gateway/input/src/run_input_gateway.cpp); 0 keeps NodeProposer's
// own default.
DEFINE_int32(max_batch_size, 0,
             "Maximum proposals batched into one ProposeBatch; 0 keeps the default (64)");
DEFINE_int32(max_inflight_batches, 0,
             "Maximum ProposeBatch RPCs in flight at once; 0 keeps the default (16). This bound is "
             "PER GATEWAY -- with several gateways feeding one leader, divide it down (see "
             "gateway/README.md)");

namespace sequencer::examples::counter {
namespace {

std::vector<std::string> splitCommaList(const std::string& s) {
  std::vector<std::string> out;
  std::size_t start = 0;
  while (start <= s.size()) {
    const std::size_t comma = s.find(',', start);
    const std::size_t end = comma == std::string::npos ? s.size() : comma;
    if (end > start) {
      out.push_back(s.substr(start, end - start));
    }
    if (comma == std::string::npos) {
      break;
    }
    start = comma + 1;
  }
  return out;
}

// CallbackService, not Service: the callback API lets a handler return
// before its answer exists, which is precisely what proposeAsync()
// needs. Under the synchronous Service there is nowhere to put an
// asynchronous completion -- the handler must block until it can
// return a Status, which is the thread-per-round-trip behaviour this
// file previously had.
class CounterSubmitServiceImpl final : public grpc_proto::CounterSubmitService::CallbackService {
 public:
  CounterSubmitServiceImpl(std::vector<std::string> nodeEndpoints, std::size_t maxBatchSize,
                            int maxInFlightBatches)
      : proposer_(std::move(nodeEndpoints), maxBatchSize, maxInFlightBatches) {}

  ::grpc::ServerUnaryReactor* SubmitDelta(::grpc::CallbackServerContext* context,
                                           const grpc_proto::SubmitDeltaRequest* request,
                                           grpc_proto::SubmitDeltaResponse* response) override {
    ::grpc::ServerUnaryReactor* reactor = context->DefaultReactor();

    // Copied, not referenced: `delta` must outlive this handler, which
    // returns as soon as the proposal is queued.
    const std::int64_t delta = request->delta();
    const sequencer::Payload payload(reinterpret_cast<const std::byte*>(&delta), sizeof(delta));

    // The handler's thread is free from here; the reactor is finished
    // on a brpc callback thread once the node answers. NodeProposer
    // copies the payload itself (see proposeAsync's comment), so the
    // local above is safe to let go.
    proposer_.proposeAsync(payload, [reactor, response](
                                         gateway::input::detail::NodeProposer::Outcome outcome) {
      if (!outcome.ok) {
        reactor->Finish(::grpc::Status(::grpc::StatusCode::UNAVAILABLE, outcome.errorMessage));
        return;
      }
      response->set_sequence_number(outcome.receipt.sequenceNumber);
      std::int64_t total = 0;
      if (outcome.designatedOutput.size() == sizeof(total)) {
        std::memcpy(&total, outcome.designatedOutput.data(), sizeof(total));
      }
      response->set_total(total);
      reactor->Finish(::grpc::Status::OK);
    });

    return reactor;
  }

 private:
  gateway::input::detail::NodeProposer proposer_;
};

}  // namespace
}  // namespace sequencer::examples::counter

int main(int argc, char** argv) {
  google::InitGoogleLogging(argv[0]);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  if (FLAGS_node_peers.empty() || FLAGS_listen_port == 0) {
    LOG(ERROR) << "--node_peers and --listen_port are required";
    return 1;
  }

  sequencer::examples::counter::CounterSubmitServiceImpl service(
      sequencer::examples::counter::splitCommaList(FLAGS_node_peers),
      static_cast<std::size_t>(FLAGS_max_batch_size), FLAGS_max_inflight_batches);

  grpc::reflection::InitProtoReflectionServerBuilderPlugin();
  grpc::ServerBuilder builder;
  builder.AddListeningPort("0.0.0.0:" + std::to_string(FLAGS_listen_port), grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
  LOG(INFO) << "counter gRPC input gateway started: listen_port=" << FLAGS_listen_port
            << " node_peers=" << FLAGS_node_peers;
  server->Wait();
  return 0;
}
