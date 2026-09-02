#pragma once

// A FIX acceptor whose session layer is QuickFIX's, not this
// repository's (specification.md §8.13).
//
// The counterpart to gateway/fix/'s FixInputTransport, and deliberately
// the same shape from the chassis's point of view: an InputTransport of
// SessionStream shape, so §8.11 governs delivery identically and the
// input codec is untouched.
//
// What differs is everything underneath. Logon, Logout, heartbeats,
// TestRequest, sequence validation, gap detection, ResendRequest,
// SequenceReset-GapFill and PossDupFlag/OrigSendingTime are QuickFIX's
// -- none of them appear here. This class is wiring: it turns
// Application callbacks into chassis calls, and keeps the session
// registry the output half needs.
//
// ONE LIMITATION, inherent to QuickFIX 1.15.1 rather than to this
// design: SocketAcceptor takes its sessions from settings.getSessions(),
// so every counterparty's CompID must be declared before it connects.
// The hffix gateway adopts identity from the Logon's SenderCompID and
// needs no such list. For a venue with known counterparties this is
// configuration; for one that accepts whoever arrives, it is a reason to
// prefer §8.12's gateway.

#include <quickfix/Application.h>
#include <quickfix/Session.h>
#include <quickfix/SessionID.h>
#include <quickfix/SessionSettings.h>
#include <quickfix/SocketAcceptor.h>

#include <sequencer/input_transport.hpp>
#include <sequencer/quickfix/journal_message_store.hpp>

#include <atomic>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace sequencer::quickfix {

struct QuickFixInputConfig {
  std::string senderCompId = "SEQUENCER";
  int heartBtInt = 30;
  // Every counterparty that may connect. See the class comment: this
  // list is required by QuickFIX, not by us.
  std::vector<std::string> clientCompIds;
  std::string sequenceStoreDir;
};

class QuickFixInputTransport : public sequencer::InputTransport, public FIX::Application {
 public:
  explicit QuickFixInputTransport(QuickFixInputConfig config);
  ~QuickFixInputTransport() override;

  QuickFixInputTransport(const QuickFixInputTransport&) = delete;
  QuickFixInputTransport& operator=(const QuickFixInputTransport&) = delete;

  // Constant, exactly as the hffix gateway's is: the transport SHAPE is
  // what §8.11 keys on, and both FIX gateways are session transports.
  sequencer::TransportShape shape() const override {
    return sequencer::TransportShape::SessionStream;
  }

  void attach(RequestFn onRequest, DisconnectFn onDisconnect) override;
  void start(int listenPort) override;
  void stop() override;

  // --- the output half's view -------------------------------------
  //
  // Deliberately NOT SessionSource: that interface hands out a
  // FixSession*, which is this repository's own session object and does
  // not exist here. QuickFIX owns the session; the output half asks
  // this class to send, rather than borrowing a session to send on.
  using SubscribeFn = std::function<void(std::uint64_t sessionId, const std::string& topic)>;
  void setSubscribeFn(SubscribeFn fn);
  using SessionReadyFn = std::function<void(std::uint64_t sessionId)>;
  void setSessionReadyFn(SessionReadyFn fn);

  std::vector<std::uint64_t> liveSessions();

  // Sends one application message on a session, recording first which
  // journal record it came from so the message store can rebuild it for
  // a resend without keeping its bytes.
  bool sendApplication(std::uint64_t sessionId, std::string_view msgType, std::string_view body,
                        std::uint64_t journalSequenceNumber, std::uint32_t outputIndex);

  // Set before start(): the store factory the acceptor is built with.
  void setStoreFactory(JournalMessageStoreFactory* factory) { storeFactory_ = factory; }

  // --- FIX::Application ---
  //
  // Public because FIX::SocketAcceptor takes an Application& and is
  // handed *this; QuickFIX calls these, nothing else should.
  void onCreate(const FIX::SessionID& sessionId) override;
  void onLogon(const FIX::SessionID& sessionId) override;
  void onLogout(const FIX::SessionID& sessionId) override;
  void toAdmin(FIX::Message& message, const FIX::SessionID& sessionId) override;
  void toApp(FIX::Message& message, const FIX::SessionID& sessionId)
      QUICKFIX_THROW(FIX::DoNotSend) override;
  void fromAdmin(const FIX::Message& message, const FIX::SessionID& sessionId)
      QUICKFIX_THROW(FIX::FieldNotFound, FIX::IncorrectDataFormat, FIX::IncorrectTagValue,
                      FIX::RejectLogon) override;
  void fromApp(const FIX::Message& message, const FIX::SessionID& sessionId)
      QUICKFIX_THROW(FIX::FieldNotFound, FIX::IncorrectDataFormat, FIX::IncorrectTagValue,
                      FIX::UnsupportedMessageType) override;

 private:
  std::uint64_t idFor(const FIX::SessionID& sessionId);
  const FIX::SessionID* sessionForId(std::uint64_t id);

  QuickFixInputConfig config_;
  RequestFn onRequest_;
  DisconnectFn onDisconnect_;
  SubscribeFn onSubscribe_;
  SessionReadyFn onSessionReady_;
  JournalMessageStoreFactory* storeFactory_ = nullptr;

  std::unique_ptr<FIX::SessionSettings> settings_;
  std::unique_ptr<FIX::LogFactory> log_;
  std::unique_ptr<FIX::SocketAcceptor> acceptor_;

  std::mutex mutex_;
  std::map<std::string, std::uint64_t> idsByKey_;
  std::map<std::uint64_t, FIX::SessionID> sessionsById_;
  std::map<std::uint64_t, bool> live_;
  std::atomic<std::uint64_t> nextId_{1};
};

}  // namespace sequencer::quickfix
