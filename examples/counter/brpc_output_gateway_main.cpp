// specification.md §10, §8.7: the counter example's output gateway,
// over brpc's own Streaming RPC — the chassis's built-in transport
// (BrpcStreamTransport), the same one output_gateway_main.cpp
// (WebSocket) and grpc_output_gateway_main.cpp (real gRPC) each
// override with something else. This is the one flavor that needed no
// override at all: RunOutputGateway's own single-codec-argument
// overload already defaults to it (sequencer/output_gateway.hpp).

#include <memory>

#include <sequencer/output_gateway.hpp>

#include "counter_output_codec.hpp"

int main(int argc, char** argv) {
  return sequencer::RunOutputGateway(argc, argv,
                                      std::make_unique<sequencer::examples::counter::CounterOutputCodec>());
}
