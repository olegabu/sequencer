// specification.md §10, §8.7: the counter example's output gateway,
// over real gRPC streaming via sequencer::GrpcOutputTransport
// (gateway/output/) — "just like" the WebSocket transport, this is a
// second parallel dissemination path for the *same* CounterOutputCodec
// and *same* running gateway chassis, unchanged: reuses
// CounterOutputCodec exactly as output_gateway_main.cpp does, so the
// JSON `{"sequence_number":N,"total":M}` bytes it already produces
// arrive here inside GrpcOutputTransport's generic
// OutputRecord.payload — see that transport's proto file comment for
// why it's a bytes envelope rather than distinct typed fields, and
// examples/counter/README.md for the tradeoff against
// grpc_input_gateway_main.cpp's fully-typed, counter-specific
// alternative pattern on the submission side.

#include <memory>

#include <sequencer/grpc_output_transport.hpp>
#include <sequencer/output_gateway.hpp>

#include "counter_output_codec.hpp"

int main(int argc, char** argv) {
  return sequencer::RunOutputGateway(
      argc, argv, std::make_unique<sequencer::examples::counter::CounterOutputCodec>(),
      [] { return std::make_unique<sequencer::GrpcOutputTransport>(); });
}
