#pragma once

// FixInputTransport — a FIX 4.4 acceptor as an InputTransport
// (specification.md §8.10 choice (b), §8.12).
//
// Composes with any application's InputCodec exactly as the brpc
// transport does: the session layer here handles Logon, sequence
// numbers, heartbeats and resends, and hands the codec whatever
// application messages arrive without interpreting them.
//
// DECLARES TransportShape::SessionStream, which is the load-bearing
// line in this file. Per specification.md §8.11 the chassis then
// withholds designated outputs from the codec entirely, and this
// transport's RequestContext::respond() puts nothing on the wire: the
// synchronous receipt is consumed for admission confirmation and
// sequence-number bookkeeping, and the client's execution reports are
// delivered by the OUTPUT side from the journal, in sequence-number
// order. That is what buys exactly-once delivery and cross-order
// ordering; see gateway/fix/README.md's "Delivery semantics".
//
// Pimpl'd: this header names no Boost.Asio or hffix type, so an
// application including it does not acquire either in its own
// translation unit -- the same reason the output transports are
// pimpl'd.

#include <cstdint>
#include <functional>
#include <memory>
#include <string>

#include <sequencer/fix/fix_session.hpp>
#include <sequencer/input_transport.hpp>

namespace sequencer::fix {

struct FixInputConfig {
  // This gateway's own CompID, and the client's. A production
  // deployment keys credentials and sessions by these.
  std::string senderCompId = "SEQUENCER";
  std::string targetCompId;  // empty accepts whatever the client claims
  int heartBtInt = 30;
  // LIMITATION, stated because it is easy to miss and expensive to
  // discover: leaving targetCompId EMPTY gives each accepted
  // connection a synthetic identity ("CLIENT<n>"), which is what makes
  // several concurrent test clients work. Setting it makes every
  // connection share one FIX session identity, and therefore one pair
  // of sequence counters -- so a second concurrent connection is
  // correctly rejected as a sequence violation.
  //
  // A real acceptor derives the identity from the Logon's own
  // SenderCompID (tag 49) and looks the counters up by it, which is how
  // a client reconnects to its own session after a drop. That is NOT
  // implemented: the session core loads its counters at construction,
  // before any Logon has been parsed, so learning the identity later
  // means re-keying the store mid-session. It is the next thing to
  // build here, and until it exists a deployment gets either one named
  // session or n anonymous ones.
  // Where the two per-session counters are persisted (§8.12: the only
  // session state that must survive a restart). Empty keeps them in
  // memory, which is correct only for tests.
  std::string sequenceStoreDir;
};

class FixInputTransport : public sequencer::InputTransport {
 public:
  explicit FixInputTransport(FixInputConfig config);
  ~FixInputTransport() override;

  FixInputTransport(const FixInputTransport&) = delete;
  FixInputTransport& operator=(const FixInputTransport&) = delete;

  // Constant, and the reason this class exists in the shape it does.
  sequencer::TransportShape shape() const override {
    return sequencer::TransportShape::SessionStream;
  }

  void attach(RequestFn onRequest, DisconnectFn onDisconnect) override;
  void start(int listenPort) override;
  void stop() override;

  // Credential check for Logon (tags 553/554). The session core calls
  // this hook; where credentials actually live is the deployment's
  // business (§8.12).
  void setAuthenticator(Authenticator authenticator);

  // Hands the output side the live session for `sessionId`, so a
  // session gateway can deliver execution reports on the very session
  // that submitted the order (§8.12 "Shape": one shared session core).
  // Returns nullptr if that session is gone.
  FixSession* sessionFor(std::uint64_t sessionId);

  struct Impl;

 private:
  std::unique_ptr<Impl> impl_;
};

}  // namespace sequencer::fix
