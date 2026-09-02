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

#include <sequencer/fix/fix_input_transport.hpp>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <memory>
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
  }
  void fromApp(const FIX::Message&, const FIX::SessionID&)
      QUICKFIX_THROW(FIX::FieldNotFound, FIX::IncorrectDataFormat, FIX::IncorrectTagValue,
                      FIX::UnsupportedMessageType) override {}

  std::atomic<int> logons{0};
  std::atomic<int> logouts{0};
  std::atomic<int> heartbeats{0};
  std::atomic<int> rejects{0};
  std::atomic<int> testRequests{0};
  std::atomic<bool> resetOnNextLogon{false};
};

// Our acceptor, with no chassis behind it: this suite tests the session
// layer, which is what QuickFIX can actually judge.
// Owns the store directory as well as the transport, so cleanup cannot
// race a live session still persisting into it -- removing it under a
// running gateway is what first exposed the unguarded store write.
struct Acceptor {
  std::unique_ptr<FixInputTransport> transport;
  std::string storeDir;

  Acceptor(int port, std::string dir, int heartBtInt = 5) : storeDir(std::move(dir)) {
    FixInputConfig config;
    config.senderCompId = "SEQUENCER";
    config.heartBtInt = heartBtInt;
    config.sequenceStoreDir = storeDir;
    transport = std::make_unique<FixInputTransport>(config);
    transport->attach([](std::shared_ptr<sequencer::RequestContext> request) { request->respond({}); },
                       [](const sequencer::SessionInfo&) {});
    transport->start(port);
  }
  ~Acceptor() {
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

std::string makeStoreDir(const char* name) {
  const std::filesystem::path dir =
      std::filesystem::temp_directory_path() /
      (std::string("qf-conformance-") + name + "-" + std::to_string(::getpid()));
  std::filesystem::remove_all(dir);
  std::filesystem::create_directories(dir);
  return dir.string();
}

TEST(QuickFixConformance, LogonAndLogoutWithARealEngine) {
  const std::string storeDir = makeStoreDir("logon");
  Acceptor acceptor(29631, storeDir);
  Initiator client(29631);
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
TEST(QuickFixConformance, HeartbeatsArriveOnTheNegotiatedInterval) {
  const std::string storeDir = makeStoreDir("heartbeat");
  Acceptor acceptor(29632, storeDir, /*heartBtInt=*/1);
  Initiator client(29632, /*heartBtInt=*/1);
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
TEST(QuickFixConformance, ATestRequestIsAnsweredWithItsId) {
  const std::string storeDir = makeStoreDir("testrequest");
  Acceptor acceptor(29633, storeDir);
  Initiator client(29633);
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
TEST(QuickFixConformance, ResetSeqNumFlagLogonIsHonoured) {
  const std::string storeDir = makeStoreDir("reset");

  {
    Acceptor acceptor(29634, storeDir);
    Initiator client(29634);
    client.start();
    ASSERT_TRUE(client.waitForLogon(std::chrono::seconds(10)));
  }

  // Reconnect with 141=Y against an acceptor whose counters carried
  // over from the session above.
  std::this_thread::sleep_for(std::chrono::seconds(2));
  Acceptor acceptor(29634, storeDir);
  Initiator client(29634);
  client.app.resetOnNextLogon.store(true);
  client.start();

  EXPECT_TRUE(client.waitForLogon(std::chrono::seconds(10)))
      << "a ResetSeqNumFlag logon must be accepted and reset both counters";
  EXPECT_EQ(client.app.rejects.load(), 0);
}

}  // namespace
}  // namespace sequencer::fix
