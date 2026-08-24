// specification.md §10: the counter example's real-gRPC submission
// path — see proto/counter_input_grpc.proto's file comment for why
// this exists as its own small service rather than a retrofit of the
// generic, byte-opaque RunInputGateway chassis. Reuses
// gateway/input's own NodeProposer (the exact leader-following retry
// logic input_gateway_main.cpp's chassis already uses internally) so
// this doesn't reimplement redirect-following from scratch.

#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "counter_input_grpc.grpc.pb.h"
#include "node_proposer.hpp"

#include <cstring>
#include <memory>
#include <string>
#include <vector>

DEFINE_string(node_peers, "", "Comma-separated raft group Propose endpoints, e.g. 127.0.0.1:8100 (required)");
DEFINE_int32(listen_port, 0, "This gRPC gateway's own client-facing port (required)");

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

class CounterSubmitServiceImpl final : public grpc_proto::CounterSubmitService::Service {
 public:
  explicit CounterSubmitServiceImpl(std::vector<std::string> nodeEndpoints) : proposer_(std::move(nodeEndpoints)) {}

  ::grpc::Status SubmitDelta(::grpc::ServerContext*, const grpc_proto::SubmitDeltaRequest* request,
                             grpc_proto::SubmitDeltaResponse* response) override {
    const std::int64_t delta = request->delta();
    const sequencer::Payload payload(reinterpret_cast<const std::byte*>(&delta), sizeof(delta));
    const gateway::input::detail::NodeProposer::Outcome outcome = proposer_.propose(payload);
    if (!outcome.ok) {
      return ::grpc::Status(::grpc::StatusCode::UNAVAILABLE, outcome.errorMessage);
    }
    response->set_sequence_number(outcome.receipt.sequenceNumber);
    std::int64_t total = 0;
    if (outcome.designatedOutput.size() == sizeof(total)) {
      std::memcpy(&total, outcome.designatedOutput.data(), sizeof(total));
    }
    response->set_total(total);
    return ::grpc::Status::OK;
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
      sequencer::examples::counter::splitCommaList(FLAGS_node_peers));

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
