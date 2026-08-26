#pragma once

// A pluggable delivery mechanism: owns accepting new output-side client
// connections and delivering to them. The chassis's built-in transport
// uses brpc's own Streaming RPC (specification.md §8.7's
// zero-additional-dependency choice); an application needing a
// different one (e.g. WebSocket, via Boost.Beast — §8.7 again, and
// examples/counter's output_gateway_main.cpp) implements this interface
// instead and passes it to RunOutputGateway's transport-factory overload.
//
// A transport is no longer a Fanout: the codec publishes into the
// chassis-owned BroadcastRing (broadcast_ring.hpp — see its file
// comment for the whole delivery design), and a transport's job is to
// give each connected subscriber its own reader — a private cursor
// into that ring, drained by a thread that subscriber effectively
// owns, filtering entries by routing tag and writing straight to the
// subscriber's own socket. No shared queues, no cross-thread write
// hand-off: the previous push-based design's per-session queues and
// wake-ups were, measured, where multi-millisecond delivery latency
// actually accumulated (examples/counter/README.md's benchmark
// section).

#include <sequencer/broadcast_ring.hpp>

namespace sequencer {

class OutputTransport {
 public:
  virtual ~OutputTransport() = default;

  // Wires this transport to the chassis's ring and topic registry
  // (both outlive the transport), and hands it the reader idle-spin
  // setting its per-subscriber readers should use (IdleStrategy's
  // constructor argument). Called exactly once, before start().
  virtual void attach(BroadcastRing& ring, TopicRegistry& topics, int idleSpinIterations) = 0;

  // Starts accepting client connections on `listenPort`. Does not block.
  virtual void start(int listenPort) = 0;

  // Stops accepting new connections and disconnects every currently
  // connected client, blocking until each one is confirmed closed —
  // reader threads joined — before returning: never leave a
  // connection's callbacks or reader able to fire after this object
  // starts being destroyed (see gateway/output/README.md's account of
  // the crash this guarantee exists to prevent).
  virtual void stop() = 0;
};

}  // namespace sequencer
