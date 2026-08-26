#pragma once

// specification.md §8.7: a real, standard-gRPC-compatible
// OutputTransport (see proto/output_grpc.proto's file comment for why
// this is a separate library from brpc, which does not implement
// genuine gRPC streaming). Same design shape as
// WebSocketOutputTransport: a real transport an application plugs into
// RunOutputGateway's transport-factory overload, generic over
// whatever bytes an OutputCodec produces.
//
// gRPC's own server reflection is enabled, so `grpcurl` (or any
// reflection-aware client) can call this service with no `.proto` file
// of its own — unlike brpc, which does not implement gRPC's reflection
// service.
//
// Pimpl'd deliberately: this header names no gRPC type, so nothing
// that merely links against sequencer::gateway_output (rather than
// this specific transport) needs the gRPC C++ library in its own
// include path — its own separate library target for exactly that
// reason (see gateway/output/CMakeLists.txt).

#include <sequencer/output_transport.hpp>

#include <memory>

namespace sequencer {

class GrpcOutputTransport : public OutputTransport {
 public:
  GrpcOutputTransport();
  ~GrpcOutputTransport() override;

  GrpcOutputTransport(const GrpcOutputTransport&) = delete;
  GrpcOutputTransport& operator=(const GrpcOutputTransport&) = delete;

  void attach(BroadcastRing& ring, TopicRegistry& topics, int idleSpinIterations) override;
  void start(int listenPort) override;
  void stop() override;

  // Public for the same reason as WebSocketOutputTransport's Impl —
  // grpc_output_transport.cpp's free-standing service class needs to
  // hold a reference to it.
  struct Impl;

 private:
  std::unique_ptr<Impl> impl_;
};

}  // namespace sequencer
