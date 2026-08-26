// specification.md §10, §8.7: the counter example's output gateway,
// over WebSocket via Boost.Beast — "the one place the example depends
// on something beyond brpc."
//
// One of three flavors, each named for its transport:
// brpc_output_gateway_main.cpp (the chassis's own built-in
// BrpcStreamTransport), grpc_output_gateway_main.cpp (real gRPC), and
// this one. They differ only in which OutputTransport they hand to
// RunOutputGateway — same CounterOutputCodec, same journal, same
// chassis.

#include <memory>

#include <sequencer/output_gateway.hpp>
#include <sequencer/websocket_output_transport.hpp>

#include "counter_output_codec.hpp"

int main(int argc, char** argv) {
  return sequencer::RunOutputGateway(
      argc, argv, std::make_unique<sequencer::examples::counter::CounterOutputCodec>(),
      [] { return std::make_unique<sequencer::WebSocketOutputTransport>(); });
}
