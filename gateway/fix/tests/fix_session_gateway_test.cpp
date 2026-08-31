// The order-entry session gateway end to end: a real single-node raft
// group, a real FIX gateway over one shared session core, and a real
// FIX client on a socket.
//
// This is where specification.md §8.11 becomes observable rather than
// structural. Two orders on ONE session -- an aggressive one that fills
// immediately, and a resting one hit later -- and the assertions are
// that the client is never answered synchronously, and that both fills
// arrive in JOURNAL order on the wire.

#include <sequencer/fix/fix_session_gateway.hpp>
#include <sequencer/fix/fix_session.hpp>

#include <boost/asio/connect.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/write.hpp>

#include <hffix_fields.hpp>

#include <atomic>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "../../../node/src/node_impl.hpp"

namespace sequencer::fix {
namespace {

namespace net = boost::asio;
using tcp = net::ip::tcp;

// An order book thin enough to be obvious: an "order" is a delta, and
// the state machine emits one execution report per order it fills. A
// resting order is filled when a later order arrives that crosses it,
// which is what produces a PASSIVE fill on an EARLIER order -- the case
// §8.11's ordering rule exists for.
class MatchingStateMachine : public sequencer::StateMachine {
 public:
  void apply(std::uint64_t sequenceNumber, sequencer::Payload input,
             sequencer::OutputCollector& outputs) override {
    std::int64_t value = 0;
    if (input.size() == sizeof(value)) {
      std::memcpy(&value, input.data(), sizeof(value));
    }

    if (value > 0) {
      // An aggressive order: fills now, and also fills any resting one.
      reports_.clear();
      reports_.push_back(makeReport(sequenceNumber, value, "AGGRESSIVE"));
      if (resting_ != 0) {
        reports_.push_back(makeReport(sequenceNumber, resting_, "PASSIVE"));
        resting_ = 0;
      }
      for (const std::string& report : reports_) {
        outputs.emit(sequencer::Payload(reinterpret_cast<const std::byte*>(report.data()),
                                         report.size()));
      }
      // Designate them all -- and watch them NOT be delivered
      // synchronously, because the transport is a session stream.
      for (std::size_t i = 0; i < reports_.size(); ++i) {
        outputs.designateOutput(i);
      }
      return;
    }

    // A resting order: acknowledged now, filled later.
    resting_ = -value;
    reports_.clear();
    reports_.push_back(makeReport(sequenceNumber, -value, "RESTING"));
    outputs.emit(sequencer::Payload(
        reinterpret_cast<const std::byte*>(reports_[0].data()), reports_[0].size()));
    outputs.designateOutput(0);
  }

  void snapshotSave(sequencer::SnapshotWriter&) override {}
  void snapshotLoad(sequencer::SnapshotReader&) override {}

 private:
  static std::string makeReport(std::uint64_t seq, std::int64_t qty, const char* kind) {
    return std::to_string(seq) + ":" + std::to_string(qty) + ":" + kind;
  }
  std::int64_t resting_ = 0;
  std::vector<std::string> reports_;
};

// Turns a FIX U1 into the 8-byte delta the state machine expects.
class FixInputCodec : public sequencer::InputCodec {
 public:
  sequencer::Result<sequencer::Bytes> toInput(const sequencer::ClientRequest& request) override {
    const std::string_view raw(reinterpret_cast<const char*>(request.body.data()),
                                request.body.size());
    hffix::message_reader reader(raw.data(), raw.size());
    if (!reader.is_complete() || !reader.is_valid()) {
      return sequencer::Result<sequencer::Bytes>::Error("not a FIX message");
    }
    std::int64_t value = 0;
    for (auto it = reader.begin(); it != reader.end(); ++it) {
      if (it->tag() == 5001) {
        value = std::strtoll(std::string(it->value().begin(), it->value().size()).c_str(),
                              nullptr, 10);
      }
    }
    sequencer::Bytes bytes(sizeof(value));
    std::memcpy(bytes.data(), &value, sizeof(value));
    return sequencer::Result<sequencer::Bytes>::Ok(std::move(bytes));
  }

  sequencer::Bytes toOutput(const sequencer::Receipt& receipt,
                             std::span<const sequencer::Payload> designatedOutputs) override {
    // Records what the chassis handed over, so the test can assert the
    // §8.11 guard fired.
    lastDesignatedCount.store(designatedOutputs.size(), std::memory_order_relaxed);
    lastSequenceNumber.store(receipt.sequenceNumber, std::memory_order_relaxed);
    return sequencer::Bytes();
  }

  std::optional<sequencer::Bytes> onDisconnect(const sequencer::SessionInfo&) override {
    return std::nullopt;
  }

  std::atomic<std::size_t> lastDesignatedCount{99};
  std::atomic<std::uint64_t> lastSequenceNumber{0};
};

// Turns each journal output into a FIX U2 addressed to the session that
// owns the order.
class FixOutputCodec : public sequencer::OutputCodec {
 public:
  void toOutput(const sequencer::journal::RecordView& record, sequencer::Fanout& fanout) override {
    for (std::size_t i = 0; i < record.outputCount(); ++i) {
      const sequencer::Payload output = record.output(i);
      const std::string text(reinterpret_cast<const char*>(output.data()), output.size());
      const std::string body = "35=U2\0015001=" + text + "\001";
      // Session 1: the single client in these tests. A real codec reads
      // the owning session from the record's own content.
      fanout.toSession(1, sequencer::Bytes(
          reinterpret_cast<const std::byte*>(body.data()),
          reinterpret_cast<const std::byte*>(body.data()) + body.size()));
    }
  }
};

class TestClient {
 public:
  TestClient(int port, std::string compId, SequenceStore* sharedStore = nullptr)
      : socket_(ioContext_) {
    tcp::resolver resolver(ioContext_);
    net::connect(socket_, resolver.resolve("127.0.0.1", std::to_string(port)));
    SessionConfig config;
    config.role = Role::Initiator;
    config.senderCompId = std::move(compId);
    config.targetCompId = "SEQUENCER";
    session_ = std::make_unique<FixSession>(config, sharedStore ? *sharedStore : store_, [] {
      return static_cast<std::uint64_t>(
          std::chrono::duration_cast<std::chrono::microseconds>(
              std::chrono::steady_clock::now().time_since_epoch()).count());
    });
    session_->setSendFn([this](std::string_view f) {
      net::write(socket_, net::buffer(f.data(), f.size()));
    });
    session_->setAppMessageFn([this](const hffix::message_reader& m) {
      for (auto it = m.begin(); it != m.end(); ++it) {
        if (it->tag() == 5001) {
          received_.emplace_back(it->value().begin(), it->value().size());
        }
        if (it->tag() == hffix::tag::PossDupFlag && it->value().size() > 0 &&
            *it->value().begin() == 'Y') {
          sawPossDup_ = true;
        }
      }
    });
  }

  ~TestClient() {
    boost::system::error_code ec;
    socket_.close(ec);
  }

  void logon() {
    session_->start();
    pumpFor(std::chrono::milliseconds(800), [this] { return session_->isLoggedOn(); });
  }
  void order(std::int64_t value) {
    session_->sendApplication("U1", "5001=" + std::to_string(value) + "\001");
  }
  // Hand-built: a ResendRequest is session-level, so FixSession has no
  // public method for it -- a real client's engine issues it on a
  // detected gap.
  void requestResend(std::uint64_t begin, std::uint64_t end) {
    session_->sendApplication("2", "7=" + std::to_string(begin) + "\00116=" +
                                        std::to_string(end) + "\001");
  }
  bool sawPossDup() const { return sawPossDup_; }
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
  const std::vector<std::string>& received() const { return received_; }

 private:
  class MemoryStore : public SequenceStore {
   public:
    SequenceNumbers load(const std::string& k) override { return n_[k]; }
    void store(const std::string& k, const SequenceNumbers& v) override { n_[k] = v; }
    std::map<std::string, SequenceNumbers> n_;
  };
  net::io_context ioContext_;
  tcp::socket socket_;
  MemoryStore store_;
  std::unique_ptr<FixSession> session_;
  std::vector<std::string> received_;
  bool sawPossDup_ = false;
};

std::filesystem::path makeTempDir() {
  std::string tmpl = (std::filesystem::temp_directory_path() / "fix_gateway_test_XXXXXX").string();
  if (::mkdtemp(tmpl.data()) == nullptr) {
    throw std::runtime_error("mkdtemp failed");
  }
  return std::filesystem::path(tmpl);
}

// The named deliverable of instruction 03 step 5: an aggressive order
// and a resting one on the SAME session, with both fills arriving in
// journal order and neither returned synchronously.
TEST(FixSessionGateway, ExecutionReportsArriveFromTheJournalInOrderNeverSynchronously) {
  const std::filesystem::path dir = makeTempDir();

  node::detail::NodeConfig nodeConfig;
  nodeConfig.groupId = "fix-gateway-test";
  nodeConfig.peerId = "127.0.0.1:29601:0";
  nodeConfig.initialPeers = nodeConfig.peerId;
  nodeConfig.dataDir = dir;
  nodeConfig.electionTimeoutMs = 300;
  node::detail::NodeImpl node(nodeConfig, std::make_unique<MatchingStateMachine>());
  node.start();
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (!node.isLeader() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  ASSERT_TRUE(node.isLeader());

  auto inputCodec = std::make_unique<FixInputCodec>();
  FixInputCodec* codec = inputCodec.get();

  SessionGatewayConfig config;
  config.nodeEndpoints = {"127.0.0.1:29601"};
  config.listenPort = 29602;
  config.dataDir = dir;
  config.resumeFile = dir / "resume";
  config.sequenceStoreDir = (dir / "seq").string();

  std::thread gatewayThread([&] {
    RunFixSessionGateway(config, std::move(inputCodec), std::make_unique<FixOutputCodec>());
  });
  std::this_thread::sleep_for(std::chrono::milliseconds(600));

  {
    TestClient client(29602, "ACME");
    client.logon();
    ASSERT_TRUE(client.isLoggedOn());

    // A resting order, then an aggressive one that crosses it. The
    // aggressive order's own fill and the passive fill on the EARLIER
    // order are both consequences of the second message.
    client.order(-5);
    client.pumpFor(std::chrono::milliseconds(600), [&] { return client.received().size() >= 1; });
    client.order(3);
    client.pumpFor(std::chrono::milliseconds(900), [&] { return client.received().size() >= 3; });

    ASSERT_GE(client.received().size(), 3u)
        << "expected a RESTING ack, then the AGGRESSIVE and PASSIVE fills";

    // §8.11's ordering guarantee: journal order on the wire. The
    // aggressive fill is emitted before the passive one within the same
    // record, and both follow the earlier acknowledgement.
    EXPECT_NE(client.received()[0].find("RESTING"), std::string::npos);
    EXPECT_NE(client.received()[1].find("AGGRESSIVE"), std::string::npos);
    EXPECT_NE(client.received()[2].find("PASSIVE"), std::string::npos)
        << "a fill on an earlier order must arrive after the aggressive one, in journal order";

    // §8.11's exactly-once half: the state machine DESIGNATED those
    // reports, and the chassis still handed the codec nothing, because
    // this transport is a session stream.
    EXPECT_EQ(codec->lastDesignatedCount.load(), 0u)
        << "designated outputs must be withheld on a session transport";
    EXPECT_GT(codec->lastSequenceNumber.load(), 0u)
        << "the receipt itself is still consumed, for bookkeeping";
  }

  std::raise(SIGTERM);
  gatewayThread.join();
  node.stop();
  std::filesystem::remove_all(dir);
}

// specification.md §8.12 reason 1, tested: a ResendRequest is served by
// RE-READING THE JOURNAL and re-running the codec, with no outbound
// message store anywhere. The messages come back at their original
// sequence numbers, flagged PossDup, carrying the original bodies.
//
// This is what the RecordOrigin added to BroadcastRing exists for --
// without the journal position travelling with each entry, the gateway
// could not know which record produced a given sent message, and the
// only correct answer to a resend would be a gap fill.
// specification.md §8.12 reason 1, end to end: a client drops, comes
// back with fresh counters, detects the gap against the gateway's
// persisted outbound number, and is caught up FROM THE JOURNAL -- the
// record re-read and the codec re-run, with no outbound message store
// anywhere.
//
// This is what the RecordOrigin on BroadcastRing and the identity-keyed
// sent map exist for. Keyed by connection instead, the returning
// session could not see its own history and the resend degraded to a
// gap fill exactly when it mattered most.
// specification.md §8.12 end to end: a client that misses an execution
// report while disconnected is caught up FROM THE JOURNAL when it
// returns -- the record re-read and the codec re-run, with no outbound
// message store anywhere.
//
// This is catch-up, NOT ResendRequest, and the difference is the whole
// lesson of getting here. The report was never sent to the absent
// session, so no outbound sequence number exists to replay and the
// client has no gap to detect. Catch-up re-reads the journal from the
// session's last persisted position and sends what it missed as new
// messages. ResendRequest covers the other case -- messages the gateway
// did send -- and is tested directly in fix_session_test.cpp.
TEST(FixSessionGateway, AClientIsCaughtUpFromTheJournalAfterReconnecting) {
  const std::filesystem::path dir = makeTempDir();

  node::detail::NodeConfig nodeConfig;
  nodeConfig.groupId = "fix-resend-test";
  nodeConfig.peerId = "127.0.0.1:29611:0";
  nodeConfig.initialPeers = nodeConfig.peerId;
  nodeConfig.dataDir = dir;
  nodeConfig.electionTimeoutMs = 300;
  node::detail::NodeImpl node(nodeConfig, std::make_unique<MatchingStateMachine>());
  node.start();
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (!node.isLeader() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  ASSERT_TRUE(node.isLeader());

  SessionGatewayConfig config;
  config.nodeEndpoints = {"127.0.0.1:29611"};
  config.listenPort = 29612;
  config.dataDir = dir;
  config.resumeFile = dir / "resume";
  config.sequenceStoreDir = (dir / "seq").string();

  std::thread gatewayThread([&] {
    RunFixSessionGateway(config, std::make_unique<FixInputCodec>(),
                          std::make_unique<FixOutputCodec>());
  });
  std::this_thread::sleep_for(std::chrono::milliseconds(600));

  // The client keeps its own counters across the drop, as a real client
  // does -- reconnecting with FRESH counters is a sequence-too-low
  // condition that FIX treats as fatal, and would test nothing about
  // resends.
  class SharedStore : public SequenceStore {
   public:
    SequenceNumbers load(const std::string& k) override { return n_[k]; }
    void store(const std::string& k, const SequenceNumbers& v) override { n_[k] = v; }
    std::map<std::string, SequenceNumbers> n_;
  } clientStore;

  {
    // The client submits an order and drops IMMEDIATELY, before the
    // execution report can reach it. The gateway commits the order and
    // sends the report to a socket that is already gone, advancing its
    // own outbound counter -- so the client is genuinely behind when it
    // returns. That is the situation a resend exists for.
    TestClient client(29612, "ACME", &clientStore);
    client.logon();
    ASSERT_TRUE(client.isLoggedOn());
    client.order(4);
  }

  // Let the gateway commit and attempt delivery to the dead session.
  std::this_thread::sleep_for(std::chrono::seconds(2));

  {
    TestClient client(29612, "ACME", &clientStore);
    client.logon();
    // Its Logon carries the sequence number it left off at, so the
    // gateway accepts it and then catches it up from the journal --
    // unprompted, because the client has no way to know what it missed.
    client.pumpFor(std::chrono::milliseconds(2000), [&] { return !client.received().empty(); });

    ASSERT_FALSE(client.received().empty())
        << "the reconnecting client must be caught up from the journal";
    EXPECT_NE(client.received().back().find("AGGRESSIVE"), std::string::npos)
        << "the missed execution report is what must arrive";

    // NOT PossDup, and this is the assertion that distinguishes catch-up
    // from a resend. PossDupFlag means "a retransmission of a message
    // previously sent"; the gateway never sent this one, because the
    // session was gone when it was published. Flagging it would tell
    // the client to treat a message it has never seen as a possible
    // duplicate, which is how a client ends up discarding a real fill.
    //
    // An earlier version of this test asserted the opposite, and was
    // wrong in the same way the code would have been.
    EXPECT_FALSE(client.sawPossDup())
        << "catch-up sends messages the gateway never sent before; they are new, not duplicates";
  }

  std::raise(SIGTERM);
  gatewayThread.join();
  node.stop();
  std::filesystem::remove_all(dir);
}

}  // namespace
}  // namespace sequencer::fix
