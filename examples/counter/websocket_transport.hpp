#pragma once

// specification.md §8.7: "brpc does not... implement the WebSocket
// protocol... A genuinely browser- and language-agnostic output
// gateway... therefore needs a separate, dedicated WebSocket library
// alongside brpc." This is that — the counter example's own
// OutputTransport, using Boost.Beast instead of the chassis's built-in
// brpc-Streaming-RPC transport. "The one place the example depends on
// something beyond brpc" (§8.7).
//
// Pimpl'd deliberately: this header names no Boost type, so nothing
// that merely links against the counter example (rather than this
// specific transport) needs Boost.Beast/Asio in its own include path.

#include <sequencer/output_transport.hpp>

#include <memory>

namespace sequencer::examples::counter {

class WebSocketTransport : public sequencer::OutputTransport {
 public:
  WebSocketTransport();
  ~WebSocketTransport() override;

  WebSocketTransport(const WebSocketTransport&) = delete;
  WebSocketTransport& operator=(const WebSocketTransport&) = delete;

  void start(int listenPort) override;
  void stop() override;

  void toSession(sequencer::SessionId owner, Bytes bytes) override;
  void broadcast(const std::string& topic, Bytes bytes) override;

  // Public so websocket_transport.cpp's free-standing Connection class
  // can hold a reference to it — its full definition lives only in that
  // .cpp regardless, so the forward declaration being visible here
  // reveals nothing.
  struct Impl;

 private:
  std::unique_ptr<Impl> impl_;
};

}  // namespace sequencer::examples::counter
