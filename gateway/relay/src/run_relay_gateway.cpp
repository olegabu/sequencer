// The relay gateway's standalone binary (specification.md §8.2, §9).
// Unlike RunInputGateway/RunOutputGateway, a relay needs no
// application codec — it carries a colocated replica's journal off its
// machine unmodified, nothing interpreted, translated, filtered, or
// reordered — so, like evidence/'s signing gateway, it ships as a
// ready-to-run stock binary rather than a library an application links.
// No application repository ever implements one; every deployment
// simply runs this binary, exactly as it would run `dumper`.
//
// Serves two independent RPC stacks, on two separate ports: the
// original brpc::Stream-based RelayService (always on), and — when
// --grpc_listen_port is set — a real, standard-gRPC RelayService too,
// for consumers that aren't brpc-aware (see relay_grpc_service_impl.hpp's
// file comment for why this needs a genuinely separate server rather
// than reusing brpc's own port: brpc and grpc::Server are two entirely
// separate server stacks, and brpc's own gRPC compatibility is
// unary-call-only, not streaming).

#include <gflags/gflags.h>
#include <sequencer/stop_signal.hpp>

#include <glog/logging.h>

#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>

#include <atomic>
#include <chrono>
#include <csignal>
#include <memory>
#include <thread>

#include "relay_gateway_impl.hpp"
#include "relay_grpc_service_impl.hpp"
#include "relay_server.hpp"

DEFINE_string(data_dir, "", "A node's journal directory to relay, colocated (required)");
DEFINE_int32(listen_port, 0, "This relay's own client-facing port, brpc::Stream-based RelayService (required)");
DEFINE_int32(grpc_listen_port, 0,
             "This relay's real-gRPC RelayService port (relay_grpc.proto). 0 (default) disables it — "
             "the brpc::Stream-based service on --listen_port always runs regardless.");

namespace sequencer::gateway::relay {
namespace {


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

  std::unique_ptr<sequencer::gateway::relay::detail::RelayGrpcServiceImpl> grpcService;
  std::unique_ptr<grpc::Server> grpcServer;
  if (FLAGS_grpc_listen_port != 0) {
    grpcService = std::make_unique<sequencer::gateway::relay::detail::RelayGrpcServiceImpl>(gateway);
    grpc::reflection::InitProtoReflectionServerBuilderPlugin();
    grpc::ServerBuilder builder;
    builder.AddListeningPort("0.0.0.0:" + std::to_string(FLAGS_grpc_listen_port), grpc::InsecureServerCredentials());
    builder.RegisterService(grpcService.get());
    grpcServer = builder.BuildAndStart();
    LOG(INFO) << "relay gRPC service started: grpc_listen_port=" << FLAGS_grpc_listen_port;
  }

  sequencer::waitForStopSignal();

  LOG(INFO) << "relay gateway stopping";
  if (grpcServer) {
    // requestStop() wakes every in-flight Subscribe() call directly
    // (see relay_grpc_service_impl.hpp); Shutdown()'s own bounded
    // deadline is just a backstop, not the primary mechanism.
    grpcService->requestStop();
    grpcServer->Shutdown(std::chrono::system_clock::now() + std::chrono::seconds(5));
    grpcServer->Wait();
  }
  server.stop();
  gateway.stop();
  return 0;
}
