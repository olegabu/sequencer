#pragma once

// A pluggable client-facing transport for the input side — the mirror
// of OutputTransport (specification.md §8.10's choice (b), resolved by
// §8.12).
//
// The chassis's built-in transport is brpc (baidu_std, brpc's own gRPC,
// and HTTP with an arbitrary body — specification.md §8.7's
// zero-additional-dependency choice). A FIX session gateway
// (gateway/fix/) implements this interface instead and is passed to
// RunInputGateway's transport-factory overload, exactly as a WebSocket
// or gRPC output transport is passed to RunOutputGateway's.
//
// The brpc path is ITSELF an implementation of this interface, not a
// special case beside it. That is deliberate: an interface whose only
// implementation is the new thing tends to be shaped around the new
// thing, and the old path then quietly diverges from it.
//
// specification.md §8.11: a gateway delivers each output to a given
// client exactly once, by the path its transport shape dictates, and
// never by both. A transport declares its shape() and the chassis
// enforces the consequence -- see TransportShape in
// <sequencer/input_codec.hpp>.

#include <cstdint>
#include <functional>
#include <memory>
#include <string>

#include <sequencer/input_codec.hpp>
#include <sequencer/payload.hpp>

namespace sequencer {

// One inbound client request, plus the means of answering it.
//
// Answering is deferred, not returned: the chassis proposes to the raft
// group asynchronously (see gateway/input/src/node_proposer.hpp on why
// a blocking handler capped throughput at workers/RTT), so a request is
// completed from a callback thread some time after the transport handed
// it over. A transport therefore keeps whatever it needs to respond --
// a brpc Controller and Closure, a FIX session and sequence number --
// alive inside its own implementation of this.
class RequestContext {
 public:
  virtual ~RequestContext() = default;

  // The raw client bytes, exactly as they arrived, for InputCodec::
  // toInput. Valid until respond()/fail() is called.
  virtual Payload body() const = 0;

  // The session this request arrived on, for transports that have
  // sessions. 0 means sessionless, which is what every request/response
  // transport reports -- there is nothing to correlate a later output
  // with, because the reply goes back on this request.
  //
  // std::uint64_t rather than the output side's SessionId alias: the
  // input side must not include the output side's headers (§9's
  // dependency rules), and they are the same type. SessionInfo::
  // sessionId is the same value.
  virtual std::uint64_t session() const { return 0; }

  // Complete the request successfully with the codec's response bytes.
  // Exactly one of respond()/fail() must be called, exactly once.
  virtual void respond(Payload response) = 0;

  // Complete the request with an error the client can see.
  virtual void fail(const std::string& message) = 0;
};

class InputTransport {
 public:
  virtual ~InputTransport() = default;

  // Fixed for the life of the transport and known before start(): the
  // chassis reads it to decide whether designated outputs may be
  // delivered as a synchronous reply at all (§8.11).
  virtual TransportShape shape() const = 0;

  using RequestFn = std::function<void(std::shared_ptr<RequestContext> request)>;
  // A session ended. Called only by transports that have sessions; the
  // chassis turns it into InputCodec::onDisconnect, which may propose a
  // disconnect input (§8.1).
  using DisconnectFn = std::function<void(const SessionInfo& session)>;

  // Wires the transport to the chassis. Called exactly once, before
  // start(). Both callbacks outlive the transport.
  virtual void attach(RequestFn onRequest, DisconnectFn onDisconnect) = 0;

  // Starts accepting client connections on `listenPort`. Does not block.
  virtual void start(int listenPort) = 0;

  // Stops accepting new connections and completes or abandons every
  // in-flight request, blocking until none can still fire a callback --
  // the same guarantee OutputTransport::stop() gives, and for the same
  // reason: a callback landing after this object begins destruction is
  // a use-after-free.
  virtual void stop() = 0;
};

}  // namespace sequencer
