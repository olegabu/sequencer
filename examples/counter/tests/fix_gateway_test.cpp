// The counter over FIX, end to end: a real single-node raft group, the
// real counter_fix_gateway binary, and a real FIX client on a socket.
//
// specification.md §10 and §8.11. CounterStateMachine DESIGNATES its
// total -- it is the submitter's own immediate consequence, and over
// REST or gRPC it comes straight back as the reply. Over FIX it must
// not: a session transport delivers every output from the journal, in
// sequence-number order, and the synchronous receipt is bookkeeping
// only. This asserts exactly that difference.

#include <sequencer/fix/fix_session.hpp>

#include <boost/asio/connect.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/write.hpp>

#include <hffix_fields.hpp>

#include <chrono>
#include <filesystem>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "child_process.hpp"
#include "counter_fix_codecs.hpp"

namespace sequencer::examples::counter {
namespace {

namespace net = boost::asio;
using tcp = net::ip::tcp;

class MemoryStore : public sequencer::fix::SequenceStore {
 public:
  sequencer::fix::SequenceNumbers load(const std::string& k) override { return n_[k]; }
  void store(const std::string& k, const sequencer::fix::SequenceNumbers& v) override { n_[k] = v; }

 private:
  std::map<std::string, sequencer::fix::SequenceNumbers> n_;
};

class FixClient {
 public:
  FixClient(int port, std::string compId) : socket_(ioContext_) {
    tcp::resolver resolver(ioContext_);
    net::connect(socket_, resolver.resolve("127.0.0.1", std::to_string(port)));
    sequencer::fix::SessionConfig config;
    config.role = sequencer::fix::Role::Initiator;
    config.senderCompId = std::move(compId);
    config.targetCompId = "SEQUENCER";
    config.heartBtInt = 30;
    session_ = std::make_unique<sequencer::fix::FixSession>(config, store_, [] {
      return static_cast<std::uint64_t>(
          std::chrono::duration_cast<std::chrono::microseconds>(
              std::chrono::steady_clock::now().time_since_epoch()).count());
    });
    session_->setSendFn([this](std::string_view f) {
      net::write(socket_, net::buffer(f.data(), f.size()));
    });
    session_->setAppMessageFn([this](const hffix::message_reader& m) {
      std::string type;
      const auto t = m.message_type();
      if (t != m.end()) {
        type.assign(t->value().begin(), t->value().size());
      }
      for (auto it = m.begin(); it != m.end(); ++it) {
        if (it->tag() == kCounterValueTag) {
          totals_.push_back({type, std::string(it->value().begin(), it->value().size())});
        }
      }
    });
  }

  ~FixClient() {
    boost::system::error_code ec;
    socket_.close(ec);
  }

  void logon() {
    session_->start();
    pumpFor(std::chrono::milliseconds(1500), [this] { return session_->isLoggedOn(); });
  }
  void subscribeToTotals() {
    // This test submits with delta 5 and -2, so the client id in the
    // high bits is 0 and its topic is TOTALS-0.
    session_->sendApplication(
        // 263=1: snapshot plus updates, the standing subscription.
        // 146=1: NoRelatedSym, the count for the one symbol below --
        // it read 10155 here, a tag that means nothing in FIX 4.4 and
        // survived only because the gateway scans for tag 55 and
        // ignores everything else in the request.
        "V", "262=req1\001263=1\001146=1\00155=" + counterTopicFor(0) + "\001");
  }
  void submit(std::int64_t delta) {
    session_->sendApplication("U1", std::to_string(kCounterValueTag) + "=" +
                                        std::to_string(delta) + "\001");
  }
  void pumpFor(std::chrono::milliseconds budget,
                const std::function<bool()>& until = [] { return false; }) {
    const auto deadline = std::chrono::steady_clock::now() + budget;
    std::vector<char> buffer(8192);
    socket_.non_blocking(true);
    while (std::chrono::steady_clock::now() < deadline) {
      boost::system::error_code ec;
      const std::size_t n = socket_.read_some(net::buffer(buffer.data(), buffer.size()), ec);
      if (!ec && n > 0) {
        session_->onBytes(std::string_view(buffer.data(), n));
      }
      if (until()) return;
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
  }
  bool isLoggedOn() const { return session_->isLoggedOn(); }
  struct Received { std::string type; std::string value; };
  const std::vector<Received>& totals() const { return totals_; }

 private:
  net::io_context ioContext_;
  tcp::socket socket_;
  MemoryStore store_;
  std::unique_ptr<sequencer::fix::FixSession> session_;
  std::vector<Received> totals_;
};

std::filesystem::path makeTempDir() {
  std::string tmpl = (std::filesystem::temp_directory_path() / "counter_fix_XXXXXX").string();
  if (::mkdtemp(tmpl.data()) == nullptr) {
    throw std::runtime_error("mkdtemp failed");
  }
  return std::filesystem::path(tmpl);
}

// Which gateway binary is under test. Both implement the same example
// -- same codecs, same journal, same §8.11 delivery -- and differ only
// in whose session layer runs underneath, so the observable behaviour a
// client sees must be identical. Running one test over both is what
// says so (gateway/quickfix/README.md).
struct GatewayUnderTest {
  const char* name;
  const char* binary;
  // QuickFIX declares its acceptor sessions up front and has no dynamic
  // ones, so the client's CompID has to be named here. The hffix
  // gateway adopts it from the Logon and needs no such argument.
  bool needsClientCompIds;
};

// Without this, gtest falls back to dumping the struct's raw bytes, and
// --gtest_list_tests annotates each case with
//   # GetParam() = 24-byte object <B5-D9 7D-A3 ...>
// CMake's gtest_discover_tests parses that listing to name its ctest
// entries and picks up the byte dump, so both cases appear in `ctest
// -N` under names that contain live POINTER VALUES -- different on
// every run under ASLR. The tests themselves are filtered by the
// correct name and do run; it is the ctest-side names that are
// unusable, which makes `ctest -R` unable to target them and CI's test
// list gratuitously nondeterministic.
inline void PrintTo(const GatewayUnderTest& gateway, std::ostream* os) { *os << gateway.name; }

class CounterFixGatewayEndToEnd : public ::testing::TestWithParam<GatewayUnderTest> {};

INSTANTIATE_TEST_SUITE_P(
    Gateways, CounterFixGatewayEndToEnd,
    ::testing::Values(GatewayUnderTest{"hffix", COUNTER_FIX_GATEWAY_MAIN_PATH, false},
                      GatewayUnderTest{"quickfix", COUNTER_QUICKFIX_GATEWAY_MAIN_PATH, true}),
    [](const ::testing::TestParamInfo<GatewayUnderTest>& info) {
      return std::string(info.param.name);
    });

TEST_P(CounterFixGatewayEndToEnd, TotalsArriveAsU2FromTheJournalNotAsTheSynchronousReply) {
  const GatewayUnderTest& gatewayUnderTest = GetParam();
  // Distinct ports per parameter: the two runs must not collide.
  const int nodePort = gatewayUnderTest.needsClientCompIds ? 29751 : 29701;
  const int fixPort = gatewayUnderTest.needsClientCompIds ? 29752 : 29702;

  const std::filesystem::path dir = makeTempDir();
  const std::string nodePeer = "127.0.0.1:" + std::to_string(nodePort) + ":0";

  ChildProcess node(COUNTER_NODE_MAIN_PATH,
                     {"--peer=" + nodePeer, "--peers=" + nodePeer,
                      "--data_dir=" + dir.string(), "--election_timeout_ms=300"});
  std::this_thread::sleep_for(std::chrono::milliseconds(900));

  std::vector<std::string> args{
      "--node_peers=127.0.0.1:" + std::to_string(nodePort),
      "--listen_port=" + std::to_string(fixPort),
      "--data_dir=" + dir.string(),
      "--resume_file=" + (dir / "fix-resume").string(),
      "--sequence_store_dir=" + (dir / "fix-seq").string()};
  if (gatewayUnderTest.needsClientCompIds) {
    args.push_back("--client_comp_ids=ACME");
  }
  ChildProcess gateway(gatewayUnderTest.binary, args);
  std::this_thread::sleep_for(std::chrono::milliseconds(900));

  FixClient client(fixPort, "ACME");
  client.logon();
  ASSERT_TRUE(client.isLoggedOn())
      << gatewayUnderTest.name << " gateway did not accept a session";

  // Subscribe first: the counter broadcasts its totals, and FIX's own
  // MarketDataRequest is the subscription (see counter_fix_codecs.hpp
  // for why the total is not addressed to the submitter).
  client.subscribeToTotals();
  client.pumpFor(std::chrono::milliseconds(500));

  client.submit(5);
  client.pumpFor(std::chrono::milliseconds(2000), [&] { return !client.totals().empty(); });
  client.submit(-2);
  client.pumpFor(std::chrono::milliseconds(2000), [&] { return client.totals().size() >= 2; });

  ASSERT_GE(client.totals().size(), 2u)
      << "the totals must reach the client -- from the journal, via the output side";
  EXPECT_EQ(client.totals()[0].type, "U2");
  EXPECT_EQ(client.totals()[0].value, "5");
  EXPECT_EQ(client.totals()[1].value, "3") << "the counter must accumulate: 5 + (-2)";

  // The §8.11 assertion, and it holds for both gateways:
  // CounterStateMachine designates its total, and over REST or gRPC that
  // designated value IS the reply. Here it is withheld, and every U2
  // above arrived from the journal instead -- which is why there are
  // exactly two of them and not four.
  EXPECT_EQ(client.totals().size(), 2u)
      << "each total must arrive exactly once: designated outputs are withheld on a "
          "session transport, so nothing is delivered twice";

  std::filesystem::remove_all(dir);
}

// The mirror of the test above, with --inline_designated_outputs on.
//
// Same observable result -- each total exactly once -- reached by the
// opposite path: the reply is built from the propose receipt and the
// journal copy is suppressed, rather than the reply being withheld and
// the journal copy delivered. Four totals here would mean the dedup
// failed and both paths delivered; zero would mean the flag turned the
// reply off without turning the inline path on.
TEST(CounterFixGateway, InlineDesignatedOutputsAnswerOnceNotTwice) {
  const std::filesystem::path dir = makeTempDir();
  const std::string nodePeer = "127.0.0.1:29711:0";

  ChildProcess node(COUNTER_NODE_MAIN_PATH,
                     {"--peer=" + nodePeer, "--peers=" + nodePeer,
                      "--data_dir=" + dir.string(), "--election_timeout_ms=300"});
  std::this_thread::sleep_for(std::chrono::milliseconds(900));

  ChildProcess gateway(COUNTER_FIX_GATEWAY_MAIN_PATH,
                        {"--node_peers=127.0.0.1:29711", "--listen_port=29712",
                         "--data_dir=" + dir.string(),
                         "--resume_file=" + (dir / "fix-resume").string(),
                         "--sequence_store_dir=" + (dir / "fix-seq").string(),
                         "--inline_designated_outputs=true"});
  std::this_thread::sleep_for(std::chrono::milliseconds(900));

  FixClient client(29712, "ACME");
  client.logon();
  ASSERT_TRUE(client.isLoggedOn()) << "the counter FIX gateway did not accept a session";

  // Subscribed exactly as in the journal-path test: the subscription is
  // what would deliver the second copy, so leaving it out would make
  // the exactly-once assertion below prove nothing.
  client.subscribeToTotals();
  client.pumpFor(std::chrono::milliseconds(500));

  client.submit(5);
  client.pumpFor(std::chrono::milliseconds(2000), [&] { return !client.totals().empty(); });
  client.submit(-2);
  client.pumpFor(std::chrono::milliseconds(2000), [&] { return client.totals().size() >= 2; });

  ASSERT_GE(client.totals().size(), 2u) << "the inline path must answer both orders";
  EXPECT_EQ(client.totals()[0].type, "U2");
  EXPECT_EQ(client.totals()[0].value, "5");
  EXPECT_EQ(client.totals()[1].value, "3") << "the counter must accumulate: 5 + (-2)";

  // Give the journal's own copy time to arrive, so this asserts the
  // dedup worked rather than that the test looked too early.
  client.pumpFor(std::chrono::milliseconds(1000));
  EXPECT_EQ(client.totals().size(), 2u)
      << "each total must arrive exactly once: the inline reply marks the journal position "
          "delivered, so the output side skips its copy";

  std::filesystem::remove_all(dir);
}

}  // namespace
}  // namespace sequencer::examples::counter
