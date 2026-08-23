// specification.md §10, §8.7: the counter example's output gateway,
// over WebSocket via Boost.Beast — "the one place the example depends
// on something beyond brpc."

#include <memory>

#include <sequencer/output_gateway.hpp>

#include "counter_output_codec.hpp"
#include "websocket_transport.hpp"

int main(int argc, char** argv) {
  return sequencer::RunOutputGateway(
      argc, argv, std::make_unique<sequencer::examples::counter::CounterOutputCodec>(),
      [] { return std::make_unique<sequencer::examples::counter::WebSocketTransport>(); });
}
