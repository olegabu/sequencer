// specification.md §10, §8.7: the counter example's output gateway,
// over WebSocket via Boost.Beast — "the one place the example depends
// on something beyond brpc."
//
// One of three flavors, each named for its transport:
// brpc_output_gateway_main.cpp (the chassis's own built-in
// BrpcOutputTransport), grpc_output_gateway_main.cpp (real gRPC), and
// this one. They differ only in which OutputTransport they hand to
// RunOutputGateway — same CounterOutputCodec, same journal, same
// chassis.

#include <memory>
#include <vector>

#include <gflags/gflags.h>

#include <sequencer/output_gateway.hpp>
#include <sequencer/websocket_output_transport.hpp>

#include "counter_output_codec.hpp"

// Defined here rather than by the chassis: a port belongs to a
// transport, and gateway/input/'s chassis already owns the
// --listen_port flag name process-wide (see output_gateway.hpp).
DEFINE_int32(listen_port, 0, "This gateway's own client-facing port (required)");

int main(int argc, char** argv) {
  return sequencer::RunOutputGateway(
      argc, argv, std::make_unique<sequencer::examples::counter::CounterOutputCodec>(), [] {
        return std::vector<sequencer::OutputTransportBinding>{
            {[] {
               return std::unique_ptr<sequencer::OutputTransport>(
                   std::make_unique<sequencer::WebSocketOutputTransport>());
             },
             FLAGS_listen_port}};
      });
}
