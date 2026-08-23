#pragma once

// A pluggable delivery mechanism: owns accepting new output-side client
// connections and delivering to them — it IS a Fanout. The chassis's
// built-in transport uses brpc's own Streaming RPC (specification.md
// §8.7's zero-additional-dependency choice); an application needing a
// different one (e.g. WebSocket, via Boost.Beast — §8.7 again, and
// examples/counter's output_gateway_main.cpp) implements this interface
// instead and passes it to RunOutputGateway's transport-factory overload.

#include <sequencer/output_codec.hpp>

namespace sequencer {

class OutputTransport : public Fanout {
 public:
  // Starts accepting client connections on `listenPort`. Does not block.
  virtual void start(int listenPort) = 0;

  // Stops accepting new connections and disconnects every currently
  // connected client, blocking until each one is confirmed closed
  // before returning — never leave a connection's callbacks able to
  // fire after this object starts being destroyed (see
  // gateway/output/README.md's account of the crash this guarantee
  // exists to prevent).
  virtual void stop() = 0;
};

}  // namespace sequencer
