// specification.md §10, §8.7: the counter example's output gateway,
// over brpc's own Streaming RPC — the chassis's built-in transport
// (BrpcOutputTransport), the same one output_gateway_main.cpp
// (websocket_output_gateway_main.cpp) and gRPC (grpc_output_gateway_main.cpp) each
// override with something else. It is the minimal single-transport
// case: BrpcOutputTransport is the chassis's own built-in
// transport, so this binary links no transport library beyond the base
// sequencer::gateway_output.

#include <memory>
#include <vector>

#include <gflags/gflags.h>

#include <sequencer/brpc_output_transport.hpp>
#include <sequencer/output_gateway.hpp>

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
                   std::make_unique<sequencer::BrpcOutputTransport>());
             },
             FLAGS_listen_port}};
      });
}
