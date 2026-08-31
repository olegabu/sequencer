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
#include <vector>

#include <sequencer/fix/fix_session.hpp>
#include <sequencer/fix/session_source.hpp>
#include <sequencer/input_transport.hpp>

namespace sequencer::fix {

struct FixInputConfig {
  // This gateway's own CompID, and the client's. A production
  // deployment keys credentials and sessions by these.
  std::string senderCompId = "SEQUENCER";
  std::string targetCompId;  // empty accepts whatever the client claims
  int heartBtInt = 30;
  // Leave EMPTY in production. An acceptor does not know who is calling
  // until the Logon arrives, and an empty value makes the session adopt
  // the peer's identity from the Logon's SenderCompID (tag 49). That is
  // what lets a client reconnect to ITS OWN session: the sequence
  // counters are keyed by the CompID pair, which survives the socket.
  //
  // Setting it pins every connection to one FIX identity, which is
  // useful only for a single-client deployment or a test. A second
  // concurrent connection claiming a live identity is refused either
  // way -- two writers on one pair of counters would corrupt both.
  // Where the two per-session counters are persisted (§8.12: the only
  // session state that must survive a restart). Empty keeps them in
  // memory, which is correct only for tests.
  std::string sequenceStoreDir;
};

class FixInputTransport : public sequencer::InputTransport, public SessionSource {
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

  // --- SessionSource (gateway/fix/output/) ---
  //
  // Implemented here so an order-entry gateway shares ONE session core
  // between its input and output halves: a client's execution reports
  // arrive on the very session that submitted the order, which is FIX's
  // convention and specification.md §8.12 "Shape"'s requirement.
  FixSession* sessionFor(std::uint64_t sessionId) override;
  std::vector<std::uint64_t> liveSessions() override;
  void setSubscribeFn(SubscribeFn fn) override;

  struct Impl;

 private:
  std::unique_ptr<Impl> impl_;
};

}  // namespace sequencer::fix
