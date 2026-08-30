// specification.md §10, §8.7: the counter example's output gateway,
// serving any combination of its three transports from one process.
//
// A port flag per protocol; 0 (the default) leaves that protocol off,
// and at least one must be set:
//
//   counter_output_gateway --data_dir=... --resume_file=...
//       --brpc_port=8600 --grpc_port=8601 --websocket_port=8602
//
// All three share one journal tail, one CounterOutputCodec pass and
// one BroadcastRing — the chassis publishes each record once no matter
// how many transports are attached (gateway/output/README.md,
// "Delivery: one ring, one reader per subscriber"), so this is
// strictly less work than running the three single-transport binaries
// side by side, which would tail the journal three times over.
//
// Note this is one process on three ports, not brpc's own one-port
// multi-protocol sniffing: brpc can do that because it implements
// those protocols itself, whereas these are three independent
// libraries (brpc, gRPC C++, Boost.Beast) each owning its own
// acceptor.
//
// The single-transport binaries still exist alongside this one:
// brpc_output_gateway_main.cpp is the minimal case, showing an
// application that wants exactly one transport and links only what
// that needs.

#include <memory>
#include <vector>

#include <gflags/gflags.h>

#include <sequencer/grpc_output_transport.hpp>
#include <sequencer/brpc_output_transport.hpp>
#include <sequencer/output_gateway.hpp>
#include <sequencer/websocket_output_transport.hpp>

#include "counter_output_codec.hpp"

DEFINE_int32(brpc_port, 0, "Serve brpc Streaming RPC subscribers on this port; 0 disables it");
DEFINE_int32(grpc_port, 0, "Serve real gRPC streaming subscribers on this port; 0 disables it");
DEFINE_int32(websocket_port, 0, "Serve WebSocket subscribers on this port; 0 disables it");

int main(int argc, char** argv) {
  return sequencer::RunOutputGateway(
      argc, argv, std::make_unique<sequencer::examples::counter::CounterOutputCodec>(),
      // Called after flag parsing, which is what lets the flags above
      // decide which transports exist at all.
      [] {
        std::vector<sequencer::OutputTransportBinding> bindings;
        if (FLAGS_brpc_port != 0) {
          // The chassis's own built-in transport is not reachable from
          // here (it lives in gateway/output/src), so the brpc flavor
          // goes through the same factory shape as the other two.
          bindings.push_back({[] {
                                return std::unique_ptr<sequencer::OutputTransport>(
                                    std::make_unique<sequencer::BrpcOutputTransport>());
                              },
                              FLAGS_brpc_port});
        }
        if (FLAGS_grpc_port != 0) {
          bindings.push_back({[] {
                                return std::unique_ptr<sequencer::OutputTransport>(
                                    std::make_unique<sequencer::GrpcOutputTransport>());
                              },
                              FLAGS_grpc_port});
        }
        if (FLAGS_websocket_port != 0) {
          bindings.push_back({[] {
                                return std::unique_ptr<sequencer::OutputTransport>(
                                    std::make_unique<sequencer::WebSocketOutputTransport>());
                              },
                              FLAGS_websocket_port});
        }
        return bindings;
      });
}
