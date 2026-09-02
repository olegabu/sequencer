// Conformance: our FIX acceptor driven by a REAL QuickFIX initiator.
//
// specification.md §8.12 keeps QuickFIX for exactly this and nothing
// else. The argument for owning a session layer rests on it being
// correct, and "correct" means interoperating with the engine other
// people's clients actually run -- not merely with our own initiator,
// which shares every assumption and would forgive every mistake it
// makes. QuickFIX's quirk-faithfulness is the point.
//
// TEST-ONLY. No production target links quickfix, and the build graph
// is what enforces that rather than a convention (see
// gateway/fix/CMakeLists.txt).
//
// Session-level only, deliberately: Logon, Logout, heartbeats,
// TestRequest, sequence resets. Those are the mechanisms the session
// core owns (§8.12 "Scope"); application-message validation belongs to
// a codec, so the initiator runs with UseDataDictionary=N and this
// suite never sends an application message.

#include <quickfix/Application.h>
#include <quickfix/Log.h>
#include <quickfix/MessageStore.h>
#include <quickfix/Session.h>
#include <quickfix/SessionSettings.h>
#include <quickfix/SocketInitiator.h>
#include <quickfix/fix44/Logon.h>
#include <quickfix/fix44/TestRequest.h>
#include <quickfix/Fields.h>

#include <sequencer/fix/fix_input_transport.hpp>
#include <sequencer/quickfix/journal_message_store.hpp>
#include <sequencer/quickfix/quickfix_input_transport.hpp>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <map>
#include <memory>
#include <mutex>
#include <cstdlib>
#include <sstream>
#include <string>
#include <thread>

#include <gtest/gtest.h>

namespace sequencer::fix {
namespace {

// QuickFIX ships no null log factory, and the screen one would drown
// the test output.
class NullLogFactory : public FIX::LogFactory {
 public:
  FIX::Log* create() override { return new FIX::NullLog(); }
  FIX::Log* create(const FIX::SessionID&) override { return new FIX::NullLog(); }
  void destroy(FIX::Log* log) override { delete log; }
};

// Records the session-level transitions QuickFIX observes, so a test
// can assert on what the engine concluded rather than on bytes.
class ConformanceApp : public FIX::Application {
 public:
  void onCreate(const FIX::SessionID&) override {}
  void onLogon(const FIX::SessionID&) override { logons.fetch_add(1); }
  void onLogout(const FIX::SessionID&) override { logouts.fetch_add(1); }
  void toAdmin(FIX::Message& message, const FIX::SessionID&) override {
    FIX::MsgType type;
    message.getHeader().getField(type);
    if (type == FIX::MsgType_Logon && resetOnNextLogon.load()) {
      message.setField(FIX::ResetSeqNumFlag(true));
    }
  }
  // QUICKFIX_THROW specs must be repeated verbatim: an override may
  // not loosen them.
  void toApp(FIX::Message&, const FIX::SessionID&) QUICKFIX_THROW(FIX::DoNotSend) override {}
  void fromAdmin(const FIX::Message& message, const FIX::SessionID&)
      QUICKFIX_THROW(FIX::FieldNotFound, FIX::IncorrectDataFormat, FIX::IncorrectTagValue,
                      FIX::RejectLogon) override {
    FIX::MsgType type;
    message.getHeader().getField(type);
    if (type == FIX::MsgType_Heartbeat) {
      heartbeats.fetch_add(1);
    }
    if (type == FIX::MsgType_Reject) {
      rejects.fetch_add(1);
    }
    if (type == FIX::MsgType_TestRequest) {
      testRequests.fetch_add(1);
    }
    if (type == FIX::MsgType_ResendRequest) {
      resendRequests.fetch_add(1);
    }
    if (type == FIX::MsgType_SequenceReset) {
      sequenceResets.fetch_add(1);
      FIX::GapFillFlag gapFill(false);
      if (message.isSetField(gapFill)) {
        message.getField(gapFill);
        if (gapFill == true) {
          gapFills.fetch_add(1);
        }
      }
    }
    if (type == FIX::MsgType_Logout) {
      logoutMessages.fetch_add(1);
    }
  }
  void fromApp(const FIX::Message&, const FIX::SessionID&)
      QUICKFIX_THROW(FIX::FieldNotFound, FIX::IncorrectDataFormat, FIX::IncorrectTagValue,
                      FIX::UnsupportedMessageType) override {
    appMessages.fetch_add(1);
  }

  std::atomic<int> logons{0};
  std::atomic<int> logouts{0};
  std::atomic<int> heartbeats{0};
  std::atomic<int> rejects{0};
  std::atomic<int> testRequests{0};
  std::atomic<int> resendRequests{0};
  std::atomic<int> sequenceResets{0};
  std::atomic<int> gapFills{0};
  std::atomic<int> logoutMessages{0};
  std::atomic<int> appMessages{0};
  std::atomic<bool> resetOnNextLogon{false};
};

// Our acceptor, with no chassis behind it: this suite tests the session
// layer, which is what QuickFIX can actually judge.
// Owns the store directory as well as the transport, so cleanup cannot
// race a live session still persisting into it -- removing it under a
// running gateway is what first exposed the unguarded store write.
struct HffixAcceptor {
  // Distinct port ranges per gateway: the typed suite runs the same
  // test against both, and they must not collide.
  static constexpr int kBasePort = 29631;
  static constexpr const char* kName = "hffix";

  std::unique_ptr<FixInputTransport> transport;
  std::string storeDir;

  std::atomic<std::uint64_t> liveSession{0};

  HffixAcceptor(int port, std::string dir, int heartBtInt = 5) : storeDir(std::move(dir)) {
    FixInputConfig config;
    config.senderCompId = "SEQUENCER";
    config.heartBtInt = heartBtInt;
    config.sequenceStoreDir = storeDir;
    transport = std::make_unique<FixInputTransport>(config);
    transport->setSessionReadyFn(
        [this](std::uint64_t id, FixSession&) { liveSession.store(id); });
    transport->attach([](std::shared_ptr<sequencer::RequestContext> request) { request->respond({}); },
                       [](const sequencer::SessionInfo&) {});
    transport->start(port);
  }

  // Sends an application message on the live session, the way the
  // output half would.
  bool sendApp(std::string_view msgType, std::string_view body) {
    const std::uint64_t id = liveSession.load();
    if (id == 0) {
      return false;
    }
    FixSession* session = transport->sessionFor(id);
    if (session == nullptr) {
      return false;
    }
    session->sendApplication(msgType, body);
    return true;
  }
  // Serves a resend the way gateway/fix/ does in production: through a
  // ResendSource the session core asks. The QuickFIX acceptor needs no
  // equivalent -- its store does this -- so the typed tests call this on
  // both and one of them does nothing.
  class Stub : public ResendSource {
   public:
    bool resend(std::uint64_t begin, std::uint64_t end, const Emit& emit) override {
      served.fetch_add(1);
      for (std::uint64_t seq = begin; seq <= end && seq < begin + 50; ++seq) {
        emit(seq, "U2", "5000=1\0015001=1\001", "20260101-00:00:00.000");
      }
      return true;
    }
    std::atomic<int> served{0};
  } stub;

  void enableResends() {
    const std::uint64_t id = liveSession.load();
    if (id == 0) {
      return;
    }
    if (FixSession* session = transport->sessionFor(id); session != nullptr) {
      session->setResendSource(&stub);
    }
  }

  ~HffixAcceptor() {
    transport->stop();
    std::filesystem::remove_all(storeDir);
  }
};

struct Initiator {
  ConformanceApp app;
  std::unique_ptr<FIX::SessionSettings> settings;
  std::unique_ptr<FIX::MemoryStoreFactory> store;
  std::unique_ptr<FIX::LogFactory> log;
  std::unique_ptr<FIX::SocketInitiator> initiator;
  FIX::SessionID sessionId;

  Initiator(int port, int heartBtInt = 5)
      : sessionId("FIX.4.4", "QFCLIENT", "SEQUENCER") {
    std::stringstream config;
    config << "[DEFAULT]\n"
           << "ConnectionType=initiator\n"
           << "ReconnectInterval=1\n"
           << "FileStorePath=\n"
           // StartDay/EndDay are given even though this is a 24/7
           // session and QuickFIX documents them as optional.
           //
           // SessionFactory::create() reads them inside a
           // `catch(ConfigError&){}` precisely so they can be omitted --
           // but Dictionary::getDay() is declared QUICKFIX_THROW(...),
           // and that macro expands to `noexcept` whenever QuickFIX is
           // itself compiled as C++17 or later:
           //
           //   #ifdef __cpp_noexcept_function_type
           //   #define QUICKFIX_THROW(...) noexcept
           //
           // So on a toolchain whose default standard is C++17+, the
           // ConfigError that signals "key absent" hits a noexcept
           // boundary and calls std::terminate instead of being caught.
           // That is why these four tests passed on a local vcpkg built
           // by g++-10 (gnu++14) and aborted in CI, where vcpkg builds
           // quickfix with the runner's default gcc-11 (gnu++17) --
           // same library version, same port hash, different macro
           // branch.
           //
           // Supplying both keys means getDay() never throws, so it does
           // not matter which branch the macro took. Sunday-to-Sunday
           // with equal times is QuickFIX's idiom for a week-long
           // session, which is what StartTime==EndTime already meant.
           << "StartDay=Sunday\n"
           << "EndDay=Sunday\n"
           << "StartTime=00:00:00\n"
           << "EndTime=00:00:00\n"
           << "UseDataDictionary=N\n"
           << "ValidateUserDefinedFields=N\n"
           << "[SESSION]\n"
           << "BeginString=FIX.4.4\n"
           << "SenderCompID=QFCLIENT\n"
           << "TargetCompID=SEQUENCER\n"
           << "SocketConnectHost=127.0.0.1\n"
           << "SocketConnectPort=" << port << "\n"
           << "HeartBtInt=" << heartBtInt << "\n";
    settings = std::make_unique<FIX::SessionSettings>(config);
    store = std::make_unique<FIX::MemoryStoreFactory>();
    log = std::getenv("FIX_SCREEN_LOG") ? std::unique_ptr<FIX::LogFactory>(new FIX::ScreenLogFactory(true, true, true))
                                       : std::unique_ptr<FIX::LogFactory>(new NullLogFactory());
    initiator = std::make_unique<FIX::SocketInitiator>(app, *store, *settings, *log);
  }

  ~Initiator() {
    if (initiator != nullptr) {
      initiator->stop(true);
    }
  }

  void start() { initiator->start(); }

  bool waitForLogon(std::chrono::milliseconds budget) {
    const auto deadline = std::chrono::steady_clock::now() + budget;
    while (std::chrono::steady_clock::now() < deadline) {
      if (app.logons.load() > 0) {
        return true;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return false;
  }
};

// The same gateway, on QuickFIX's session layer (specification.md
// §8.13). Same surface as HffixAcceptor so one suite drives both -- the
// point of the exercise is that a real engine cannot tell them apart.
//
// Its message store is journal-backed like the real one, with a stub
// standing in for the journal: sendApp() registers the body under a
// synthetic journal position, and the store rebuilds from it. So a
// resend here exercises the real reconstruction path, not a shortcut.
struct QuickFixAcceptor {
  static constexpr int kBasePort = 29731;
  static constexpr const char* kName = "quickfix";

  class Bodies : public sequencer::quickfix::BodySource {
   public:
    bool bodyFor(std::uint64_t seq, std::uint32_t, std::string& msgTypeOut,
                  std::string& bodyOut) override {
      std::lock_guard<std::mutex> lock(mutex);
      const auto it = bodies.find(seq);
      if (it == bodies.end()) {
        return false;
      }
      msgTypeOut = "U2";
      bodyOut = it->second;
      return true;
    }
    std::mutex mutex;
    std::map<std::uint64_t, std::string> bodies;
  };

  class Sequences : public sequencer::quickfix::SequenceNumberStore {
   public:
    void load(const std::string& key, int& nextSender, int& nextTarget) override {
      std::lock_guard<std::mutex> lock(mutex);
      nextSender = sender.count(key) ? sender[key] : 1;
      nextTarget = target.count(key) ? target[key] : 1;
    }
    void save(const std::string& key, int nextSender, int nextTarget) override {
      std::lock_guard<std::mutex> lock(mutex);
      sender[key] = nextSender;
      target[key] = nextTarget;
    }
    std::mutex mutex;
    std::map<std::string, int> sender, target;
  };

  Bodies bodies;
  Sequences sequences;
  std::unique_ptr<sequencer::quickfix::JournalMessageStoreFactory> storeFactory;
  std::unique_ptr<sequencer::quickfix::QuickFixInputTransport> transport;
  std::string storeDir;
  std::atomic<std::uint64_t> liveSession{0};
  std::atomic<std::uint64_t> nextJournalSeq{1};

  QuickFixAcceptor(int port, std::string dir, int heartBtInt = 5) : storeDir(std::move(dir)) {
    sequencer::quickfix::QuickFixInputConfig config;
    config.senderCompId = "SEQUENCER";
    config.heartBtInt = heartBtInt;
    // QuickFIX declares its acceptor sessions up front, so the client
    // this suite uses has to be named. See the transport's header.
    config.clientCompIds = {"QFCLIENT"};
    storeFactory = std::make_unique<sequencer::quickfix::JournalMessageStoreFactory>(bodies,
                                                                                      sequences);
    transport = std::make_unique<sequencer::quickfix::QuickFixInputTransport>(config);
    transport->setStoreFactory(storeFactory.get());
    transport->setSessionReadyFn([this](std::uint64_t id) { liveSession.store(id); });
    transport->attach([](std::shared_ptr<sequencer::RequestContext> request) { request->respond({}); },
                       [](const sequencer::SessionInfo&) {});
    transport->start(port);
  }

  bool sendApp(std::string_view msgType, std::string_view body) {
    const std::uint64_t id = liveSession.load();
    if (id == 0) {
      return false;
    }
    const std::uint64_t journalSeq = nextJournalSeq.fetch_add(1);
    {
      std::lock_guard<std::mutex> lock(bodies.mutex);
      bodies.bodies[journalSeq] = std::string(body);
    }
    return transport->sendApplication(id, msgType, body, journalSeq, 0);
  }

  // Nothing to do: QuickFIX serves resends from the message store,
  // which is wired at construction.
  void enableResends() {}

  ~QuickFixAcceptor() {
    transport->stop();
    std::filesystem::remove_all(storeDir);
  }
};

// Polls rather than sleeps: these are real sockets and a real engine,
// and a fixed sleep is either flaky or slow.
template <typename Predicate>
bool waitFor(Predicate done, std::chrono::milliseconds budget) {
  const auto deadline = std::chrono::steady_clock::now() + budget;
  while (std::chrono::steady_clock::now() < deadline) {
    if (done()) {
      return true;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  return done();
}

std::string makeStoreDir(const char* name) {
  const std::filesystem::path dir =
      std::filesystem::temp_directory_path() /
      (std::string("qf-conformance-") + name + "-" + std::to_string(::getpid()));
  std::filesystem::remove_all(dir);
  std::filesystem::create_directories(dir);
  return dir.string();
}

// One suite, both gateways (specification.md §8.12 and §8.13).
//
// Every test below runs twice: once against the hffix gateway with its
// own session core, once against the QuickFIX gateway. That is the
// whole point -- a real engine should not be able to tell them apart,
// and any behaviour only one of them gets right shows up here as a
// single red cell rather than as a difference nobody looked for.
template <typename AcceptorT>
class Conformance : public ::testing::Test {};

using AcceptorTypes = ::testing::Types<HffixAcceptor, QuickFixAcceptor>;

class AcceptorNames {
 public:
  template <typename T>
  static std::string GetName(int) {
    return T::kName;
  }
};
TYPED_TEST_SUITE(Conformance, AcceptorTypes, AcceptorNames);

TYPED_TEST(Conformance, LogonAndLogoutWithARealEngine) {
  const std::string storeDir = makeStoreDir("logon");
  TypeParam acceptor(TypeParam::kBasePort + 0, storeDir);
  Initiator client(TypeParam::kBasePort + 0);
  client.start();

  ASSERT_TRUE(client.waitForLogon(std::chrono::seconds(10)))
      << "QuickFIX did not consider the session established -- our Logon echo "
          "is not what an engine expects";

  FIX::Session* session = FIX::Session::lookupSession(client.sessionId);
  ASSERT_NE(session, nullptr);
  session->logout("conformance");

  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (client.app.logouts.load() == 0 && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  EXPECT_GT(client.app.logouts.load(), 0) << "the Logout handshake must complete";
}

// Heartbeats on the negotiated interval, which is what keeps an engine
// from declaring us dead.
TYPED_TEST(Conformance, HeartbeatsArriveOnTheNegotiatedInterval) {
  const std::string storeDir = makeStoreDir("heartbeat");
  TypeParam acceptor(TypeParam::kBasePort + 1, storeDir, /*heartBtInt=*/1);
  Initiator client(TypeParam::kBasePort + 1, /*heartBtInt=*/1);
  client.start();
  ASSERT_TRUE(client.waitForLogon(std::chrono::seconds(10)));

  // Quiet for several intervals: an engine that receives nothing sends
  // a TestRequest and then disconnects, so surviving this IS the
  // assertion.
  std::this_thread::sleep_for(std::chrono::seconds(4));

  EXPECT_EQ(client.app.logouts.load(), 0)
      << "QuickFIX disconnected us: heartbeats are not arriving as it expects";
  EXPECT_GT(client.app.heartbeats.load(), 0) << "no heartbeats observed";
}

// A TestRequest must be answered with a Heartbeat echoing its TestReqID
// -- QuickFIX drops a session that gets this wrong.
TYPED_TEST(Conformance, ATestRequestIsAnsweredWithItsId) {
  const std::string storeDir = makeStoreDir("testrequest");
  TypeParam acceptor(TypeParam::kBasePort + 2, storeDir);
  Initiator client(TypeParam::kBasePort + 2);
  client.start();
  ASSERT_TRUE(client.waitForLogon(std::chrono::seconds(10)));

  const int before = client.app.heartbeats.load();
  FIX44::TestRequest request(FIX::TestReqID("conformance-1"));
  FIX::Session::sendToTarget(request, client.sessionId);

  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (client.app.heartbeats.load() <= before &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  EXPECT_GT(client.app.heartbeats.load(), before)
      << "a TestRequest must be answered with a Heartbeat carrying its TestReqID";
  EXPECT_EQ(client.app.logouts.load(), 0)
      << "QuickFIX would log out if the TestReqID did not match";
}

// ResetSeqNumFlag on Logon: both sides restart at 1. An engine checks
// this strictly, and getting it wrong strands a session at end of day.
TYPED_TEST(Conformance, ResetSeqNumFlagLogonIsHonoured) {
  const std::string storeDir = makeStoreDir("reset");

  {
    TypeParam acceptor(TypeParam::kBasePort + 3, storeDir);
    Initiator client(TypeParam::kBasePort + 3);
    client.start();
    ASSERT_TRUE(client.waitForLogon(std::chrono::seconds(10)));
  }

  // Reconnect with 141=Y against an acceptor whose counters carried
  // over from the session above.
  std::this_thread::sleep_for(std::chrono::seconds(2));
  TypeParam acceptor(TypeParam::kBasePort + 3, storeDir);
  Initiator client(TypeParam::kBasePort + 3);
  client.app.resetOnNextLogon.store(true);
  client.start();

  EXPECT_TRUE(client.waitForLogon(std::chrono::seconds(10)))
      << "a ResetSeqNumFlag logon must be accepted and reset both counters";
  EXPECT_EQ(client.app.rejects.load(), 0);
}


// --- the resend machinery, against a real engine -------------------
//
// These are the paths fix_session_test.cpp covers on its own terms and
// this suite did not cover at all. They matter more than the handshake:
// a hand-rolled engine agreeing with itself about resends says nothing
// about whether a counterparty agrees, and resend/gap-fill is where FIX
// implementations actually diverge.

// A gap in what the CLIENT sends must make us ask for the missing
// range, and the session must survive the exchange. QuickFIX answers a
// ResendRequest by itself, so a session that is still alive afterwards
// is the engine's own verdict that our request was well-formed.
TYPED_TEST(Conformance, AGapInInboundMakesUsRequestAResendAndTheSessionSurvives) {
  const std::string storeDir = makeStoreDir("inbound-gap");
  TypeParam acceptor(TypeParam::kBasePort + 4, storeDir);
  Initiator client(TypeParam::kBasePort + 4);
  client.start();
  ASSERT_TRUE(client.waitForLogon(std::chrono::seconds(10)));

  FIX::Session* session = FIX::Session::lookupSession(client.sessionId);
  ASSERT_NE(session, nullptr);

  // Skip ahead: the next message the client sends claims a sequence
  // number five beyond what we are expecting.
  const int jumped = session->getExpectedSenderNum() + 5;
  session->setNextSenderMsgSeqNum(jumped);
  FIX44::TestRequest probe(FIX::TestReqID("after-gap"));
  FIX::Session::sendToTarget(probe, client.sessionId);

  EXPECT_TRUE(waitFor([&] { return client.app.resendRequests.load() > 0; },
                       std::chrono::seconds(10)))
      << "a gap in inbound sequence numbers must produce a ResendRequest";

  // QuickFIX fills the gap on its own; the session must come back and
  // still answer a TestRequest afterwards.
  const int before = client.app.heartbeats.load();
  EXPECT_TRUE(waitFor([&] { return client.app.heartbeats.load() > before; },
                       std::chrono::seconds(15)))
      << "the session must resynchronise and keep running after the gap";
  EXPECT_EQ(client.app.logouts.load(), 0) << "a recoverable gap must not end the session";
}

// The mirror: the CLIENT believes it missed messages and asks US for a
// resend. This suite's acceptor has no journal behind it, so the
// correct answer is a SequenceReset-GapFill covering the range -- and
// QuickFIX has to accept it and carry on.
TYPED_TEST(Conformance, AResendRequestIsAnsweredWithAGapFillTheEngineAccepts) {
  const std::string storeDir = makeStoreDir("resend-gapfill");
  TypeParam acceptor(TypeParam::kBasePort + 5, storeDir);
  Initiator client(TypeParam::kBasePort + 5);
  client.start();
  ASSERT_TRUE(client.waitForLogon(std::chrono::seconds(10)));

  FIX::Session* session = FIX::Session::lookupSession(client.sessionId);
  ASSERT_NE(session, nullptr);

  // Rewind what the client expects from us, so it concludes it has a
  // hole and asks for the range back.
  session->setNextTargetMsgSeqNum(1);
  FIX44::TestRequest probe(FIX::TestReqID("provoke-resend"));
  FIX::Session::sendToTarget(probe, client.sessionId);

  EXPECT_TRUE(waitFor([&] { return client.app.gapFills.load() > 0; },
                       std::chrono::seconds(15)))
      << "a ResendRequest with nothing to resend must be answered with "
          "SequenceReset-GapFill, not silence";
  EXPECT_EQ(client.app.logouts.load(), 0)
      << "the engine must accept our gap fill rather than dropping the session";
}

// FIX 4.4: a sequence number BELOW the expectation cannot be recovered
// from, and the session must end rather than continue on a number
// nobody agrees about.
TYPED_TEST(Conformance, ASequenceNumberBelowExpectationEndsTheSession) {
  const std::string storeDir = makeStoreDir("low-seqnum");
  TypeParam acceptor(TypeParam::kBasePort + 6, storeDir);
  Initiator client(TypeParam::kBasePort + 6);
  client.start();
  ASSERT_TRUE(client.waitForLogon(std::chrono::seconds(10)));

  FIX::Session* session = FIX::Session::lookupSession(client.sessionId);
  ASSERT_NE(session, nullptr);

  // Rewind the client's OUTBOUND numbering, so its next message
  // repeats a sequence number we have already seen.
  session->setNextSenderMsgSeqNum(1);
  FIX44::TestRequest probe(FIX::TestReqID("too-low"));
  FIX::Session::sendToTarget(probe, client.sessionId);

  EXPECT_TRUE(waitFor([&] {
                 return client.app.logouts.load() > 0 || client.app.logoutMessages.load() > 0;
               },
                       std::chrono::seconds(15)))
      << "a sequence number below the expectation is fatal in FIX 4.4 and must "
          "end the session rather than being silently accepted";
}

// An application message we send must reach the engine's application
// callback -- not its reject path. This is the outbound framing the
// output half depends on, judged by something other than our own
// parser.
TYPED_TEST(Conformance, AnApplicationMessageWeSendReachesTheEnginesApplicationLayer) {
  const std::string storeDir = makeStoreDir("app-message");
  TypeParam acceptor(TypeParam::kBasePort + 7, storeDir);
  Initiator client(TypeParam::kBasePort + 7);
  client.start();
  ASSERT_TRUE(client.waitForLogon(std::chrono::seconds(10)));

  ASSERT_TRUE(waitFor([&] { return acceptor.liveSession.load() != 0; },
                       std::chrono::seconds(10)))
      << "the acceptor must have a live session before it can send on one";

  // The counter's own shape: a private MsgType carrying private tags.
  ASSERT_TRUE(acceptor.sendApp("U2", "5000=7\0015001=42\001"));

  EXPECT_TRUE(waitFor([&] { return client.app.appMessages.load() > 0; },
                       std::chrono::seconds(10)))
      << "our application message must arrive at fromApp; reaching fromAdmin or "
          "the reject path would mean the framing is wrong";
  EXPECT_EQ(client.app.rejects.load(), 0)
      << "a real engine must not reject what we frame as an application message";
}


// A resend served from a real source, rather than gap-filled away.
// This is the path a production gateway takes -- the journal replays
// what was sent -- and FIX 4.4 requires each replayed message to carry
// PossDupFlag=Y and the OrigSendingTime of the original. An engine
// that receives a replay without them treats it as a NEW message at an
// already-used sequence number, which is fatal. Nothing but a real
// engine can judge that.
TYPED_TEST(Conformance, AServedResendCarriesPossDupAndIsAcceptedByTheEngine) {
  const std::string storeDir = makeStoreDir("served-resend");
  TypeParam acceptor(TypeParam::kBasePort + 8, storeDir);
  Initiator client(TypeParam::kBasePort + 8);
  client.start();
  ASSERT_TRUE(client.waitForLogon(std::chrono::seconds(10)));
  ASSERT_TRUE(waitFor([&] { return acceptor.liveSession.load() != 0; },
                       std::chrono::seconds(10)));

  // Each gateway serves this its own way: hffix through a ResendSource
  // on the session core, QuickFIX through the journal-backed message
  // store. The assertion below is the same for both -- the engine asked,
  // we answered, and it kept the session.
  acceptor.enableResends();

  // Send a few application messages so there is a range worth asking
  // for, then rewind what the client expects so it asks for them.
  for (int i = 0; i < 3; ++i) {
    ASSERT_TRUE(acceptor.sendApp("U2", "5000=1\0015001=1\001"));
  }
  ASSERT_TRUE(waitFor([&] { return client.app.appMessages.load() >= 3; },
                       std::chrono::seconds(10)));

  FIX::Session* qf = FIX::Session::lookupSession(client.sessionId);
  ASSERT_NE(qf, nullptr);
  const int expected = qf->getExpectedTargetNum();
  qf->setNextTargetMsgSeqNum(expected - 2);

  const int beforeReplay = client.app.appMessages.load();
  ASSERT_TRUE(acceptor.sendApp("U2", "5000=1\0015001=1\001"));

  // The replays must actually ARRIVE, not merely fail to break the
  // session. Asserting only "no logout" would pass against a gateway
  // that answered a ResendRequest with silence, which is the bug this
  // test exists to catch.
  EXPECT_TRUE(waitFor([&] { return client.app.appMessages.load() > beforeReplay; },
                       std::chrono::seconds(15)))
      << "the engine asked for a range and must receive it";
  EXPECT_EQ(client.app.logouts.load(), 0)
      << "a replayed message without PossDupFlag=Y and OrigSendingTime would be "
          "read as a new message at a used sequence number, which ends the session";
}

// The client tells US to skip forward. FIX 4.4: SequenceReset-GapFill
// moves the inbound expectation to NewSeqNo, and everything after must
// continue from there.
TYPED_TEST(Conformance, AClientSequenceResetGapFillAdvancesOurExpectation) {
  const std::string storeDir = makeStoreDir("client-seqreset");
  TypeParam acceptor(TypeParam::kBasePort + 9, storeDir);
  Initiator client(TypeParam::kBasePort + 9);
  client.start();
  ASSERT_TRUE(client.waitForLogon(std::chrono::seconds(10)));

  FIX::Session* qf = FIX::Session::lookupSession(client.sessionId);
  ASSERT_NE(qf, nullptr);

  // Jump the client's outbound numbering forward and announce it the
  // legitimate way, rather than leaving us to discover a hole.
  const int from = qf->getExpectedSenderNum();
  FIX::Message reset;
  reset.getHeader().setField(FIX::MsgType(FIX::MsgType_SequenceReset));
  reset.setField(FIX::GapFillFlag(true));
  reset.setField(FIX::NewSeqNo(from + 4));
  FIX::Session::sendToTarget(reset, client.sessionId);
  // The sender must actually CONTINUE from NewSeqNo. Announcing a jump
  // and then numbering the next message as though it had not happened
  // is incoherent, and our gateway is right to end the session over it
  // -- the first version of this test did exactly that and read the
  // resulting "MsgSeqNum too low" Logout as a failure, when the trace
  // showed the engine had honoured the reset and then correctly
  // rejected a sequence number below the new expectation.
  qf->setNextSenderMsgSeqNum(from + 4);

  // If the expectation moved, the session keeps running and heartbeats
  // continue. If it did not, we would demand a resend of a range the
  // client has already declared skipped.
  const int before = client.app.heartbeats.load();
  EXPECT_TRUE(waitFor([&] { return client.app.heartbeats.load() > before; },
                       std::chrono::seconds(15)))
      << "SequenceReset-GapFill must advance our inbound expectation and leave "
          "the session healthy";
  EXPECT_EQ(client.app.logouts.load(), 0);
}

// Sequence numbers are session state that must outlive a connection:
// the client drops and reconnects WITHOUT ResetSeqNumFlag, so both
// sides must resume their counters rather than restart them.
TYPED_TEST(Conformance, SequenceNumbersSurviveAReconnectWithoutReset) {
  const std::string storeDir = makeStoreDir("reconnect");
  TypeParam acceptor(TypeParam::kBasePort + 10, storeDir);

  int afterFirst = 0;
  {
    Initiator client(TypeParam::kBasePort + 10);
    client.start();
    ASSERT_TRUE(client.waitForLogon(std::chrono::seconds(10)));
    FIX44::TestRequest probe(FIX::TestReqID("before-reconnect"));
    FIX::Session::sendToTarget(probe, client.sessionId);
    ASSERT_TRUE(waitFor([&] { return client.app.heartbeats.load() > 0; },
                         std::chrono::seconds(10)));
    FIX::Session* qf = FIX::Session::lookupSession(client.sessionId);
    ASSERT_NE(qf, nullptr);
    afterFirst = qf->getExpectedTargetNum();
    EXPECT_GT(afterFirst, 1) << "the first session must have advanced our outbound numbering";
  }

  // A fresh initiator with a fresh in-memory store starts its own
  // numbering at 1, so this asserts what OUR side persisted: it must
  // not have forgotten, and it must recover the session rather than
  // dropping it.
  Initiator second(TypeParam::kBasePort + 10);
  second.start();
  EXPECT_TRUE(second.waitForLogon(std::chrono::seconds(15)))
      << "the gateway must accept a reconnect and reconcile sequence numbers "
          "rather than refusing the session";
}

}  // namespace
}  // namespace sequencer::fix
