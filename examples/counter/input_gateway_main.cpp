// specification.md §10: the counter example's input gateway —
// baidu_std/gRPC natively via brpc, plus HTTP with an arbitrary JSON
// body (see gateway/input/proto/input_gateway.proto's comment for how).

#include <memory>

#include <sequencer/input_gateway.hpp>

#include "counter_input_codec.hpp"

int main(int argc, char** argv) {
  return sequencer::RunInputGateway(argc, argv,
                                     std::make_unique<sequencer::examples::counter::CounterInputCodec>());
}
