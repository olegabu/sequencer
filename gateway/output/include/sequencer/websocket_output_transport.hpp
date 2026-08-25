#pragma once

// specification.md §8.7: "brpc does not... implement the WebSocket
// protocol... A genuinely browser- and language-agnostic output
// gateway... therefore needs a separate, dedicated WebSocket library
// alongside brpc." This is that — a ready-to-use OutputTransport built
// on Boost.Beast, for any application that wants one, not just
// examples/counter (which originally owned this and now just links
// it — see examples/counter/README.md).
//
// Topic routing: WebSocket has no subscribe handshake of its own the
// way brpc's Subscribe RPC does, so a connecting client's topic is
// taken from the WebSocket URL's request path, leading slash
// stripped — connecting to `ws://host:port/totals` joins the "totals"
// topic, exactly as if the client had called
// Fanout::broadcast("totals", ...)'s matching subscribe. A client
// connecting to `ws://host:port/` (empty path) joins the empty-string
// topic, which simply never matches unless an application deliberately
// broadcasts to "".
//
// Pimpl'd deliberately: this header names no Boost type, so nothing
// that merely links against sequencer::gateway_output (rather than
// this specific transport) needs Boost.Beast/Asio in its own include
// path — this is its own separate library target for exactly that
// reason (see gateway/output/CMakeLists.txt).

#include <sequencer/output_transport.hpp>

#include <memory>

namespace sequencer {

class WebSocketOutputTransport : public OutputTransport {
 public:
  WebSocketOutputTransport();
  ~WebSocketOutputTransport() override;

  WebSocketOutputTransport(const WebSocketOutputTransport&) = delete;
  WebSocketOutputTransport& operator=(const WebSocketOutputTransport&) = delete;

  void start(int listenPort) override;
  void stop() override;

  void toSession(SessionId owner, Bytes bytes) override;
  void broadcast(const std::string& topic, Bytes bytes) override;
  void flush() override;

  // Public so websocket_output_transport.cpp's free-standing
  // Connection class can hold a reference to it — its full definition
  // lives only in that .cpp regardless, so the forward declaration
  // being visible here reveals nothing.
  struct Impl;

 private:
  std::unique_ptr<Impl> impl_;
};

}  // namespace sequencer
