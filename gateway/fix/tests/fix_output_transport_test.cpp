// FixOutputTransport delivering onto live FIX sessions owned by
// FixInputTransport -- the order-entry shape of specification.md
// §8.12, where both halves share ONE session core so a client's
// execution reports arrive on the session that submitted the order.

#include <sequencer/fix/fix_input_transport.hpp>
#include <sequencer/fix/fix_output_transport.hpp>

#include <boost/asio/connect.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/write.hpp>

#include <hffix_fields.hpp>

#include <atomic>
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

class TestClient {
 public:
  TestClient(int port, std::string compId) : socket_(ioContext_) {
    tcp::resolver resolver(ioContext_);
    net::connect(socket_, resolver.resolve("127.0.0.1", std::to_string(port)));
    SessionConfig config;
    config.role = Role::Initiator;
    config.senderCompId = std::move(compId);
    config.targetCompId = "SEQUENCER";
    session_ = std::make_unique<FixSession>(config, store_, [] {
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
      std::string payload;
      for (auto it = m.begin(); it != m.end(); ++it) {
        if (it->tag() == 5001) {
          payload.assign(it->value().begin(), it->value().size());
        }
      }
      received_.push_back({type, payload});
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
  void send(std::string_view type, std::string_view body) {
    session_->sendApplication(type, body);
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
  struct Message { std::string type; std::string payload; };
  const std::vector<Message>& received() const { return received_; }

 private:
  net::io_context ioContext_;
  tcp::socket socket_;
  MemorySequenceStore store_;
  std::unique_ptr<FixSession> session_;
  std::vector<Message> received_;
};

// Both halves of an order-entry gateway, sharing one session core.
struct SessionGateway {
  std::unique_ptr<FixInputTransport> input;
  std::unique_ptr<FixOutputTransport> output;
  sequencer::BroadcastRing ring{1024, 512};
  sequencer::TopicRegistry topics;
  std::atomic<int> requests{0};
  std::vector<std::uint64_t> sessionIds;
  std::mutex mutex;

  explicit SessionGateway(int port) {
    FixInputConfig config;
    config.senderCompId = "SEQUENCER";
    input = std::make_unique<FixInputTransport>(config);
    output = std::make_unique<FixOutputTransport>(*input);
    output->attach(ring, topics, 50);
    input->attach(
        [this](std::shared_ptr<sequencer::RequestContext> request) {
          {
            std::lock_guard<std::mutex> lock(mutex);
            sessionIds.push_back(request->session());
          }
          requests.fetch_add(1);
        },
        [](const sequencer::SessionInfo&) {});
    input->start(port);
    output->start(0);
  }

  ~SessionGateway() {
    output->stop();
    input->stop();
  }

  // What OutputGatewayImpl's RingFanout does: publish a tagged entry.
  void publishToSession(std::uint64_t sessionId, const std::string& body) {
    ring.publish(sequencer::makeSessionTag(sessionId),
                  reinterpret_cast<const std::byte*>(body.data()), body.size());
  }
  void publishBroadcast(const std::string& topic, const std::string& body) {
    ring.publish(sequencer::makeTopicTag(topics.idFor(topic)),
                  reinterpret_cast<const std::byte*>(body.data()), body.size());
  }
};

TEST(FixOutputTransport, DeliversAnOutputOnTheSessionThatSubmittedTheOrder) {
  SessionGateway gateway(29501);
  TestClient client(29501, "ACME");
  client.logon();
  ASSERT_TRUE(client.isLoggedOn());

  client.send("U1", "5001=42\001");
  client.pumpFor(std::chrono::milliseconds(500), [&] { return gateway.requests.load() >= 1; });
  ASSERT_EQ(gateway.requests.load(), 1);

  const std::uint64_t sessionId = gateway.sessionIds.front();
  // §8.12 "Shape": the report comes back on the ORDER-ENTRY session.
  gateway.publishToSession(sessionId, "35=U2\0015001=99\001");
  client.pumpFor(std::chrono::milliseconds(500), [&] { return !client.received().empty(); });

  ASSERT_EQ(client.received().size(), 1u);
  EXPECT_EQ(client.received()[0].type, "U2");
  EXPECT_EQ(client.received()[0].payload, "99");
}

// The ordering guarantee §8.11 exists for: a client's outputs across
// DIFFERENT orders arrive in journal order, because one reader walks
// the ring once and dispatches in sequence.
TEST(FixOutputTransport, OutputsForOneSessionArriveInJournalOrder) {
  SessionGateway gateway(29502);
  TestClient client(29502, "ACME");
  client.logon();
  client.send("U1", "5001=1\001");
  client.pumpFor(std::chrono::milliseconds(500), [&] { return gateway.requests.load() >= 1; });
  const std::uint64_t sessionId = gateway.sessionIds.front();

  for (int i = 1; i <= 5; ++i) {
    gateway.publishToSession(sessionId, "35=U2\0015001=" + std::to_string(i) + "\001");
  }
  client.pumpFor(std::chrono::milliseconds(800), [&] { return client.received().size() >= 5; });

  ASSERT_EQ(client.received().size(), 5u);
  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ(client.received()[i].payload, std::to_string(i + 1))
        << "outputs must arrive in journal order, not interleaved";
  }
}

// §8.10's topic question, resolved FIX's own way: no MarketDataRequest,
// no delivery.
TEST(FixOutputTransport, BroadcastsReachOnlySessionsThatSubscribed) {
  SessionGateway gateway(29503);
  TestClient subscriber(29503, "ALPHA");
  subscriber.logon();
  TestClient bystander(29503, "BETA");
  bystander.logon();
  ASSERT_TRUE(subscriber.isLoggedOn());
  ASSERT_TRUE(bystander.isLoggedOn());

  // A MarketDataRequest naming one symbol.
  subscriber.send("V", "262=req1\001146=1\00155=TOTALS\001");
  subscriber.pumpFor(std::chrono::milliseconds(400),
                      [&] { return gateway.requests.load() >= 1; });

  gateway.publishBroadcast("TOTALS", "35=W\0015001=tick\001");
  subscriber.pumpFor(std::chrono::milliseconds(500),
                      [&] { return !subscriber.received().empty(); });
  bystander.pumpFor(std::chrono::milliseconds(200));

  ASSERT_EQ(subscriber.received().size(), 1u);
  EXPECT_EQ(subscriber.received()[0].payload, "tick");
  EXPECT_TRUE(bystander.received().empty())
      << "a session that sent no MarketDataRequest must receive nothing";
}

TEST(FixOutputTransport, SentMessagesAreRecordedForResend) {
  SessionGateway gateway(29504);
  TestClient client(29504, "ACME");
  client.logon();
  client.send("U1", "5001=1\001");
  client.pumpFor(std::chrono::milliseconds(500), [&] { return gateway.requests.load() >= 1; });
  const std::uint64_t sessionId = gateway.sessionIds.front();

  gateway.publishToSession(sessionId, "35=U2\0015001=7\001");
  client.pumpFor(std::chrono::milliseconds(500), [&] { return !client.received().empty(); });

  // The (session, outbound seqNum) -> what-was-sent map a ResendRequest
  // consults. See the transport's own note on what this record does and
  // does not yet carry.
  EXPECT_GE(gateway.output->sentRecordCount(sessionId), 1u);
}

}  // namespace
}  // namespace sequencer::fix
