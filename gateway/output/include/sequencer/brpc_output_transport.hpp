#pragma once

// specification.md §8.7: the chassis's built-in OutputTransport, over
// brpc's own Streaming RPC.
//
// Same design shape as GrpcOutputTransport and WebSocketOutputTransport
// — a transport an application plugs into RunOutputGateway's
// transport-factory overload, generic over whatever bytes an
// OutputCodec produces — and deliberately the same shape in its
// declaration too, so the three flavors are told apart by their
// transport and by nothing else.
//
// Pimpl'd for the same reason as the other two: this header names no
// brpc type, so including it does not drag brpc/server.h into an
// application's translation unit. (Unlike gRPC and WebSocket, brpc is
// not optional for this library — the chassis uses it regardless — so
// here the pimpl buys compile time rather than an optional dependency.)

#include <sequencer/output_transport.hpp>

#include <memory>

namespace sequencer {

class BrpcOutputTransport : public OutputTransport {
 public:
  BrpcOutputTransport();
  ~BrpcOutputTransport() override;

  BrpcOutputTransport(const BrpcOutputTransport&) = delete;
  BrpcOutputTransport& operator=(const BrpcOutputTransport&) = delete;

  void attach(BroadcastRing& ring, TopicRegistry& topics, int idleSpinIterations) override;
  void start(int listenPort) override;
  void stop() override;

  // Public for the same reason as the other two transports' Impl: the
  // implementation file's own types need to name it.
  struct Impl;

 private:
  std::unique_ptr<Impl> impl_;
};

}  // namespace sequencer
