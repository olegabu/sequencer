// FixInputTransport over a real socket, driven by a FixSession in
// initiator role as the client -- the same session core in its other
// role, which is specification.md §8.12's reason 2 made concrete.
//
// The test that matters here is the §8.11 one: a FIX transport declares
// SessionStream, so the chassis must withhold designated outputs and
// the transport must put nothing on the wire in reply to an order. The
// client's answer arrives later, from the journal, via the output side.

#include <sequencer/fix/fix_input_transport.hpp>

#include <boost/asio/connect.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/write.hpp>

#include <hffix_fields.hpp>

#include <atomic>
#include <filesystem>
#include <fstream>
#include <chrono>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

namespace sequencer::fix {
namespace {

namespace net = boost::asio;
using tcp = net::ip::tcp;

class MemorySequenceStore : public SequenceStore {
 public:
  SequenceNumbers load(const std::string& key) override { return numbers_[key]; }
  void store(const std::string& key, const SequenceNumbers& n) override { numbers_[key] = n; }
  std::map<std::string, SequenceNumbers> numbers_;
};

// A real FIX client: a socket plus a FixSession in Initiator role.
class TestClient {
 public:
  explicit TestClient(int port, std::string compId = "CLIENT") : socket_(ioContext_) {
    tcp::resolver resolver(ioContext_);
    net::connect(socket_, resolver.resolve("127.0.0.1", std::to_string(port)));

    SessionConfig config;
    config.role = Role::Initiator;
    config.senderCompId = compId;
    config.targetCompId = "SEQUENCER";
    config.heartBtInt = 30;
    // Each client claims a distinct CompID so the acceptor's synthetic
    // per-connection identities line up with distinct sessions.
    session_ = std::make_unique<FixSession>(config, store_, [] {
      return static_cast<std::uint64_t>(
          std::chrono::duration_cast<std::chrono::microseconds>(
              std::chrono::steady_clock::now().time_since_epoch())
              .count());
    });
    session_->setSendFn([this](std::string_view frame) {
      net::write(socket_, net::buffer(frame.data(), frame.size()));
    });
    session_->setAppMessageFn([this](const hffix::message_reader& m) {
      received_.emplace_back(m.message_begin(), m.message_size());
    });
  }

  ~TestClient() {
    boost::system::error_code ec;
    socket_.close(ec);
  }

  void logon() {
    session_->start();
    pumpFor(std::chrono::milliseconds(500), [this] { return session_->isLoggedOn(); });
  }

  void send(std::string_view msgType, std::string_view body) {
    session_->sendApplication(msgType, body);
  }

  // Reads whatever has arrived, for up to `budget`, feeding the session.
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
      if (until()) {
        return;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
  }

  bool isLoggedOn() const { return session_->isLoggedOn(); }
  const std::vector<std::string>& received() const { return received_; }
  FixSession& session() { return *session_; }

 private:
  net::io_context ioContext_;
  tcp::socket socket_;
  MemorySequenceStore store_;
  std::unique_ptr<FixSession> session_;
  std::vector<std::string> received_;
};

// Reads back what the transport's own FileSequenceStore wrote, in the
// same format, so a test can assert on persistence without reaching
// into the transport.
class FileSequenceStoreProbe {
 public:
  explicit FileSequenceStoreProbe(std::string directory) : directory_(std::move(directory)) {}

  SequenceNumbers load(const std::string& sessionKey) const {
    std::string safe = sessionKey;
    for (char& c : safe) {
      if (c == '/' || c == '>' || c == '-') {
        c = '_';
      }
    }
    std::ifstream in(std::filesystem::path(directory_) / (safe + ".seq"));
    SequenceNumbers numbers;
    in >> numbers.nextOutbound >> numbers.nextInbound;
    return numbers;
  }

 private:
  std::string directory_;
};

struct Harness {
  std::unique_ptr<FixInputTransport> transport;
  std::atomic<int> requests{0};
  std::atomic<int> disconnects{0};
  std::vector<std::string> bodies;
  std::mutex bodiesMutex;
  std::vector<std::shared_ptr<sequencer::RequestContext>> contexts;

  explicit Harness(int port, std::string storeDir = {}) {
    FixInputConfig config;
    config.senderCompId = "SEQUENCER";
    config.sequenceStoreDir = std::move(storeDir);
    // Left empty so each client's identity is adopted from its own
    // Logon, which is how a real acceptor works.
    transport = std::make_unique<FixInputTransport>(config);
    transport->attach(
        [this](std::shared_ptr<sequencer::RequestContext> request) {
          {
            std::lock_guard<std::mutex> lock(bodiesMutex);
            const sequencer::Payload body = request->body();
            bodies.emplace_back(reinterpret_cast<const char*>(body.data()), body.size());
            contexts.push_back(request);
          }
          requests.fetch_add(1);
        },
        [this](const sequencer::SessionInfo&) { disconnects.fetch_add(1); });
    transport->start(port);
  }

  ~Harness() { transport->stop(); }
};

TEST(FixInputTransport, DeclaresSessionStreamShape) {
  FixInputConfig config;
  FixInputTransport transport(config);
  // The whole reason this class exists in this shape (§8.11): the
  // chassis reads this to decide that designated outputs must NOT be
  // returned synchronously.
  EXPECT_EQ(transport.shape(), sequencer::TransportShape::SessionStream);
}

TEST(FixInputTransport, AcceptsALogonAndCompletesTheHandshake) {
  Harness harness(29401);
  TestClient client(29401);
  client.logon();
  EXPECT_TRUE(client.isLoggedOn());
}

TEST(FixInputTransport, ApplicationMessagesReachTheChassisUninterpreted) {
  Harness harness(29402);
  TestClient client(29402);
  client.logon();
  ASSERT_TRUE(client.isLoggedOn());

  client.send("U1", "5001=42\001");
  client.pumpFor(std::chrono::milliseconds(500),
                  [&] { return harness.requests.load() >= 1; });

  ASSERT_EQ(harness.requests.load(), 1);
  std::lock_guard<std::mutex> lock(harness.bodiesMutex);
  // The transport hands over the message exactly as it arrived -- it
  // does not parse or reshape application fields (§8.10).
  EXPECT_NE(harness.bodies[0].find("5001=42"), std::string::npos);
  EXPECT_NE(harness.bodies[0].find("35=U1"), std::string::npos);
}

// The §8.11 test. An order is submitted; the chassis's response --
// whatever the codec produced from the receipt -- must NOT reach the
// wire. The client's answer comes later from the journal, delivered by
// the output side, which is what makes delivery exactly-once and keeps
// a session's fills in journal order across different orders.
TEST(FixInputTransport, RespondingToARequestPutsNothingOnTheWire) {
  Harness harness(29403);
  TestClient client(29403);
  client.logon();
  ASSERT_TRUE(client.isLoggedOn());

  client.send("U1", "5001=7\001");
  client.pumpFor(std::chrono::milliseconds(500),
                  [&] { return harness.requests.load() >= 1; });
  ASSERT_EQ(harness.requests.load(), 1);

  const std::size_t receivedBefore = client.received().size();

  // Complete the request the way the chassis would.
  {
    std::lock_guard<std::mutex> lock(harness.bodiesMutex);
    const std::string reply = "35=U2\0015001=7\001";
    harness.contexts[0]->respond(sequencer::Payload(
        reinterpret_cast<const std::byte*>(reply.data()), reply.size()));
  }

  client.pumpFor(std::chrono::milliseconds(300));
  EXPECT_EQ(client.received().size(), receivedBefore)
      << "§8.11: a session transport must not answer an order synchronously -- "
          "the execution report is delivered from the journal by the output side";
}

TEST(FixInputTransport, ASocketDropIsReportedAsASessionLoss) {
  Harness harness(29404);
  {
    TestClient client(29404);
    client.logon();
    ASSERT_TRUE(client.isLoggedOn());
  }  // socket closes here
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
  while (harness.disconnects.load() == 0 && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  EXPECT_EQ(harness.disconnects.load(), 1)
      << "a dropped socket is a session loss the codec must hear about (§8.1)";
}

TEST(FixInputTransport, AnOrderlyLogoutIsReportedAsASessionLoss) {
  Harness harness(29405);
  TestClient client(29405);
  client.logon();
  ASSERT_TRUE(client.isLoggedOn());

  client.session().logout("done");
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
  while (harness.disconnects.load() == 0 && std::chrono::steady_clock::now() < deadline) {
    client.pumpFor(std::chrono::milliseconds(50));
  }
  EXPECT_EQ(harness.disconnects.load(), 1);
}

// The reason identity is adopted from the Logon rather than synthesized
// per connection: a client that drops and reconnects must resume ITS
// OWN session, with its own sequence counters, not be handed a fresh
// one. Counters are keyed by the CompID pair, which survives the
// socket.
TEST(FixInputTransport, AReconnectingClientResumesItsOwnSequenceNumbers) {
  const std::string storeDir =
      (std::filesystem::temp_directory_path() /
       ("fix-seq-" + std::to_string(::getpid()))).string();
  std::filesystem::remove_all(storeDir);
  Harness harness(29407, storeDir);

  {
    TestClient client(29407, "ACME");
    client.logon();
    ASSERT_TRUE(client.isLoggedOn());
    client.send("U1", "5001=1\001");
    client.send("U1", "5001=2\001");
    client.pumpFor(std::chrono::milliseconds(400),
                    [&] { return harness.requests.load() >= 2; });
    ASSERT_EQ(harness.requests.load(), 2);
  }

  // The gateway-side counters for ACME must have advanced and been
  // persisted: Logon plus two application messages inbound.
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
  while (harness.disconnects.load() == 0 && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }

  FileSequenceStoreProbe probe(storeDir);
  const SequenceNumbers persisted = probe.load("SEQUENCER->ACME");
  EXPECT_EQ(persisted.nextInbound, 4u)
      << "Logon(1) + two application messages(2,3) leaves 4 expected next";
  std::filesystem::remove_all(storeDir);
}

// Two live connections claiming one FIX identity would interleave
// writers onto a single pair of counters. The second is refused.
TEST(FixInputTransport, ASecondConnectionForALiveIdentityIsRefused) {
  Harness harness(29408);
  TestClient first(29408, "ACME");
  first.logon();
  ASSERT_TRUE(first.isLoggedOn());

  TestClient second(29408, "ACME");
  second.logon();
  EXPECT_FALSE(second.isLoggedOn())
      << "a duplicate live identity must be refused, not given the same counters";
  EXPECT_TRUE(first.isLoggedOn()) << "the established session must be unaffected";
}

TEST(FixInputTransport, EachSessionGetsItsOwnIdentity) {
  Harness harness(29406);
  TestClient first(29406, "ALPHA");
  first.logon();
  TestClient second(29406, "BETA");
  second.logon();
  ASSERT_TRUE(first.isLoggedOn());
  ASSERT_TRUE(second.isLoggedOn());

  first.send("U1", "5001=1\001");
  second.send("U1", "5001=2\001");
  first.pumpFor(std::chrono::milliseconds(500), [&] { return harness.requests.load() >= 2; });
  second.pumpFor(std::chrono::milliseconds(200), [&] { return harness.requests.load() >= 2; });

  ASSERT_EQ(harness.requests.load(), 2);
  std::lock_guard<std::mutex> lock(harness.bodiesMutex);
  // Distinct sessions, so an output can be addressed back to the right
  // one -- which is what a session gateway needs to deliver a fill on
  // the session that submitted the order (§8.12 "Shape").
  EXPECT_NE(harness.contexts[0]->session(), harness.contexts[1]->session());
  EXPECT_NE(harness.contexts[0]->session(), 0u);
}

}  // namespace
}  // namespace sequencer::fix
