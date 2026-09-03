#include <sequencer/quickfix/quickfix_input_transport.hpp>

#include <quickfix/Log.h>
#include <quickfix/Values.h>
#include <quickfix/fix44/MarketDataRequest.h>

#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <sstream>

namespace sequencer::quickfix {
namespace {

// QuickFIX ships no null log factory and the screen one would flood a
// gateway's stderr at rate.
class NullLogFactory : public FIX::LogFactory {
 public:
  FIX::Log* create() override { return new FIX::NullLog(); }
  FIX::Log* create(const FIX::SessionID&) override { return new FIX::NullLog(); }
  void destroy(FIX::Log* log) override { delete log; }
};

// One inbound application message, handed to the chassis exactly as it
// arrived (§8.10: the transport does not interpret it).
//
// respond() SENDS NOTHING, for the same reason gateway/fix/'s does:
// §8.11 makes the journal the only delivery path on a session
// transport, and returning the receipt here too would be the
// exactly-once violation the rule exists to prevent.
class QuickFixRequestContext : public sequencer::RequestContext {
 public:
  QuickFixRequestContext(std::string body, std::uint64_t sessionId)
      : body_(std::move(body)), sessionId_(sessionId) {}

  sequencer::Payload body() const override {
    return sequencer::Payload(reinterpret_cast<const std::byte*>(body_.data()), body_.size());
  }
  std::uint64_t session() const override { return sessionId_; }
  void respond(sequencer::Payload) override {}
  void fail(const std::string&) override {}

 private:
  std::string body_;
  std::uint64_t sessionId_;
};

}  // namespace

QuickFixInputTransport::QuickFixInputTransport(QuickFixInputConfig config)
    : config_(std::move(config)) {}

QuickFixInputTransport::~QuickFixInputTransport() { stop(); }

void QuickFixInputTransport::attach(RequestFn onRequest, DisconnectFn onDisconnect) {
  onRequest_ = std::move(onRequest);
  onDisconnect_ = std::move(onDisconnect);
}

void QuickFixInputTransport::setSubscribeFn(SubscribeFn fn) { onSubscribe_ = std::move(fn); }
void QuickFixInputTransport::setSessionReadyFn(SessionReadyFn fn) {
  onSessionReady_ = std::move(fn);
}

void QuickFixInputTransport::start(int listenPort) {
  // One [SESSION] per declared counterparty. QuickFIX 1.15.1 has no
  // dynamic acceptor sessions, so this list is the whole guest list --
  // see the header.
  std::stringstream config;
  config << "[DEFAULT]\n"
         << "ConnectionType=acceptor\n"
         << "SocketAcceptPort=" << listenPort << "\n"
         << "FileStorePath=\n"
         // A week-long session. StartDay/EndDay are supplied even though
         // they are documented as optional, because Dictionary::getDay()
         // is declared QUICKFIX_THROW(...) and that macro becomes
         // `noexcept` under C++17 -- so the ConfigError meaning "key
         // absent" reaches a noexcept boundary and calls std::terminate
         // instead of being caught. Supplying them means it never throws.
         << "StartDay=Sunday\n"
         << "EndDay=Sunday\n"
         << "StartTime=00:00:00\n"
         << "EndTime=00:00:00\n"
         << "UseDataDictionary=N\n"
         << "ValidateUserDefinedFields=N\n"
         << "ResetOnLogon=N\n"
         // Nagle is left ON here, deliberately, and this is the one
         // transport in the repository where that is the right answer.
         //
         // TCP_NODELAY is safe when the transport batches its own
         // writes: gateway/fix/ accumulates into outBuffer and drains
         // it in one syscall, so un-Nagling costs nothing and removed a
         // 40ms delayed-ACK stall. QuickFIX does NOT batch -- it writes
         // per message -- so SocketNodelay=Y multiplies its packet rate
         // instead, and measurement is unambiguous about the result:
         //
         //   100k, SocketNodelay unset:  p50   958us,      0 dropped
         //   100k, SocketNodelay=Y:      p50 1,082us, 29,559 dropped
         //   ceiling, unset:  ~158k     ceiling, =Y:  ~123k
         //
         // So the trade is a rare ~41ms tail sample against a 22%
         // ceiling. Nagle is doing useful work for this transport, and
         // the correct fix is application-level write coalescing inside
         // the gateway, not turning Nagle off underneath a library that
         // does not coalesce.
         ;
  for (const std::string& client : config_.clientCompIds) {
    config << "[SESSION]\n"
           << "BeginString=FIX.4.4\n"
           << "SenderCompID=" << config_.senderCompId << "\n"
           << "TargetCompID=" << client << "\n"
           << "HeartBtInt=" << config_.heartBtInt << "\n";
  }

  settings_ = std::make_unique<FIX::SessionSettings>(config);
  // QUICKFIX_SCREEN_LOG=1 puts every message on stderr. Off by
  // default -- at rate this would flood a gateway -- but it is the only
  // way to see what QuickFIX was processing when something inside it
  // threw.
  log_ = std::getenv("QUICKFIX_SCREEN_LOG") != nullptr
             ? std::unique_ptr<FIX::LogFactory>(new FIX::ScreenLogFactory(true, true, true))
             : std::unique_ptr<FIX::LogFactory>(new NullLogFactory());
  acceptor_ = std::make_unique<FIX::ThreadedSocketAcceptor>(*this, *storeFactory_, *settings_, *log_);
  acceptor_->start();
}

void QuickFixInputTransport::stop() {
  if (acceptor_ != nullptr) {
    acceptor_->stop(true);
    acceptor_.reset();
  }
}

std::uint64_t QuickFixInputTransport::idFor(const FIX::SessionID& sessionId) {
  const std::string key = sessionId.toString();
  std::lock_guard<std::mutex> lock(mutex_);
  const auto it = idsByKey_.find(key);
  if (it != idsByKey_.end()) {
    return it->second;
  }
  const std::uint64_t id = nextId_.fetch_add(1);
  idsByKey_[key] = id;
  sessionsById_.emplace(id, sessionId);
  return id;
}

const FIX::SessionID* QuickFixInputTransport::sessionForId(std::uint64_t id) {
  std::lock_guard<std::mutex> lock(mutex_);
  const auto it = sessionsById_.find(id);
  return it == sessionsById_.end() ? nullptr : &it->second;
}

std::vector<std::uint64_t> QuickFixInputTransport::liveSessions() {
  std::lock_guard<std::mutex> lock(mutex_);
  std::vector<std::uint64_t> ids;
  for (const auto& [id, isLive] : live_) {
    if (isLive) {
      ids.push_back(id);
    }
  }
  return ids;
}

void QuickFixInputTransport::onCreate(const FIX::SessionID& sessionId) { idFor(sessionId); }

void QuickFixInputTransport::onLogon(const FIX::SessionID& sessionId) {
  const std::uint64_t id = idFor(sessionId);
  {
    std::lock_guard<std::mutex> lock(mutex_);
    live_[id] = true;
  }
  if (onSessionReady_) {
    onSessionReady_(id);
  }
}

void QuickFixInputTransport::onLogout(const FIX::SessionID& sessionId) {
  const std::uint64_t id = idFor(sessionId);
  {
    std::lock_guard<std::mutex> lock(mutex_);
    live_[id] = false;
  }
  if (onDisconnect_) {
    sequencer::SessionInfo info;
    info.sessionId = id;
    onDisconnect_(info);
  }
}

void QuickFixInputTransport::toAdmin(FIX::Message&, const FIX::SessionID&) {}

void QuickFixInputTransport::toApp(FIX::Message&, const FIX::SessionID&)
    QUICKFIX_THROW(FIX::DoNotSend) {}

void QuickFixInputTransport::fromAdmin(const FIX::Message&, const FIX::SessionID&)
    QUICKFIX_THROW(FIX::FieldNotFound, FIX::IncorrectDataFormat, FIX::IncorrectTagValue,
                    FIX::RejectLogon) {}

void QuickFixInputTransport::fromApp(const FIX::Message& message, const FIX::SessionID& sessionId)
    QUICKFIX_THROW(FIX::FieldNotFound, FIX::IncorrectDataFormat, FIX::IncorrectTagValue,
                    FIX::UnsupportedMessageType) {
  // NOTHING may escape this function.
  //
  // QUICKFIX_THROW expands to `noexcept` whenever QuickFIX is compiled
  // as C++17 or later, so the exception specification in the signature
  // above is a lie: an escaping FIX::FieldNotFound does not reach
  // QuickFIX's handler, it calls std::terminate and takes the gateway
  // down. A missing field in a client's message must never do that.
  try {
    fromAppGuarded(message, sessionId);
  } catch (const FIX::Exception& e) {
    std::fprintf(stderr, "[quickfix-gateway] dropped a malformed application message: %s\n",
                 e.what());
  } catch (const std::exception& e) {
    std::fprintf(stderr, "[quickfix-gateway] dropped an application message: %s\n", e.what());
  }
}

void QuickFixInputTransport::fromAppGuarded(const FIX::Message& message,
                                             const FIX::SessionID& sessionId) {
  const std::uint64_t id = idFor(sessionId);

  // EVERY field lookup below is guarded with isSetField, and none of
  // them is wrapped in a try. That is not defensive style, it is the
  // only thing that works.
  //
  // FIX::Message::getField is declared QUICKFIX_THROW(FieldNotFound),
  // and that macro expands to `noexcept` whenever QuickFIX is compiled
  // as C++17 or later. So on a missing field getField does not throw a
  // catchable exception -- it calls std::terminate INSIDE ITSELF,
  // before any catch of ours can see it. A try/catch around it is
  // structurally incapable of helping; this cost a crash and a
  // debugging session to establish.
  //
  // The rule for this whole directory: never call a QuickFIX accessor
  // that is documented to throw. Ask isSetField first.
  FIX::MsgType msgType;
  if (!message.getHeader().isSetField(msgType)) {
    return;  // no MsgType: nothing sensible to do, and asking would kill us
  }
  message.getHeader().getField(msgType);

  // The same one exception the hffix gateway makes: MarketDataRequest
  // is a subscription, which is how §8.10's topic question is answered
  // FIX's own way. It is still passed to the codec afterwards.
  if (msgType.getValue() == FIX::MsgType_MarketDataRequest && onSubscribe_) {
    // SubscriptionRequestType (263) says which of the three requests
    // this is; see SessionSource::SubscriptionAction. Read with
    // isSetField first, like every other accessor in this directory --
    // and defaulting to Snapshot when it is absent or unrecognised, so
    // the fallback is the one that registers nothing.
    using Action = sequencer::fix::SessionSource::SubscriptionAction;
    Action action = Action::Snapshot;
    FIX::SubscriptionRequestType requestType;
    if (message.isSetField(requestType)) {
      message.getField(requestType);
      if (requestType.getValue() == FIX::SubscriptionRequestType_SNAPSHOT_PLUS_UPDATES) {
        action = Action::Subscribe;
      } else if (requestType.getValue() ==
                 FIX::SubscriptionRequestType_DISABLE_PREVIOUS_SNAPSHOT_PLUS_UPDATE_REQUEST) {
        action = Action::Unsubscribe;
      }
    }
    // Symbols are read by scanning for tag 55 rather than through
    // repeating-group accessors: the group API needs a data dictionary,
    // this session runs without one (UseDataDictionary=N), and the
    // gateway must not care how a client laid the request out. The
    // hffix gateway scans for the same tag for the same reason.
    const std::string raw = message.toString();
    std::size_t pos = 0;
    while ((pos = raw.find("\00155=", pos)) != std::string::npos) {
      const std::size_t start = pos + 4;
      const std::size_t soh = raw.find('\001', start);
      if (soh == std::string::npos) {
        break;
      }
      onSubscribe_(id, raw.substr(start, soh - start), action);
      pos = soh;
    }
  }

  if (onRequest_) {
    onRequest_(std::make_shared<QuickFixRequestContext>(message.toString(), id));
  }
}

bool QuickFixInputTransport::sendApplication(std::uint64_t sessionId, std::string_view msgType,
                                              std::string_view body,
                                              std::uint64_t journalSequenceNumber,
                                              std::uint32_t outputIndex) {
  const FIX::SessionID* target = sessionForId(sessionId);
  if (target == nullptr) {
    return false;
  }

  // Provenance BEFORE the send: QuickFIX will call the store's set()
  // from inside sendToTarget, and set() sees only a sequence number and
  // bytes. This is how the row it records knows which journal record to
  // rebuild from (see journal_message_store.hpp).
  if (storeFactory_ != nullptr) {
    if (JournalMessageStore* store = storeFactory_->storeFor(*target); store != nullptr) {
      store->noteOrigin(journalSequenceNumber, outputIndex);
    }
  }

  FIX::Message message;
  message.getHeader().setField(FIX::MsgType(std::string(msgType)));
  std::size_t pos = 0;
  while (pos < body.size()) {
    const std::size_t soh = body.find('\001', pos);
    if (soh == std::string_view::npos) {
      break;
    }
    const std::string_view field = body.substr(pos, soh - pos);
    const std::size_t eq = field.find('=');
    if (eq != std::string_view::npos) {
      message.setField(FIX::FieldBase(std::atoi(std::string(field.substr(0, eq)).c_str()),
                                       std::string(field.substr(eq + 1))));
    }
    pos = soh + 1;
  }
  // sendToTarget throws SessionNotFound if the session went away
  // between the lookup above and here, which is a race the output
  // thread must survive rather than die of.
  try {
    return FIX::Session::sendToTarget(message, *target);
  } catch (const FIX::Exception& e) {
    std::fprintf(stderr, "[quickfix-gateway] send failed: %s\n", e.what());
    return false;
  }
}

}  // namespace sequencer::quickfix
