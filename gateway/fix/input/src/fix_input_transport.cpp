#include <sequencer/fix/fix_input_transport.hpp>

#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/write.hpp>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <map>
#include <set>
#include <mutex>
#include <thread>
#include <vector>

namespace sequencer::fix {
namespace {

namespace net = boost::asio;
using tcp = net::ip::tcp;

std::uint64_t steadyMicros() {
  return static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count());
}

// The two counters per session, on disk. specification.md §8.12: this
// is the ONLY session state that must persist -- everything else about
// what was sent is re-derived from the journal.
class FileSequenceStore : public SequenceStore {
 public:
  explicit FileSequenceStore(std::string directory) : directory_(std::move(directory)) {
    if (!directory_.empty()) {
      std::filesystem::create_directories(directory_);
    }
  }

  SequenceNumbers load(const std::string& sessionKey) override {
    std::lock_guard<std::mutex> lock(mutex_);
    if (directory_.empty()) {
      return memory_[sessionKey];
    }
    std::ifstream in(pathFor(sessionKey));
    SequenceNumbers numbers;
    if (in >> numbers.nextOutbound >> numbers.nextInbound) {
      return numbers;
    }
    return SequenceNumbers{};
  }

  void store(const std::string& sessionKey, const SequenceNumbers& numbers) override {
    std::lock_guard<std::mutex> lock(mutex_);
    if (directory_.empty()) {
      memory_[sessionKey] = numbers;
      return;
    }
    // Write-and-rename: a torn counter file after a crash would resume
    // a session at the wrong sequence number, which FIX cannot recover
    // from without an operator resetting both sides.
    const std::filesystem::path finalPath = pathFor(sessionKey);
    const std::filesystem::path tempPath = finalPath.string() + ".tmp";
    {
      std::ofstream out(tempPath, std::ios::trunc);
      out << numbers.nextOutbound << ' ' << numbers.nextInbound << '\n';
      out.flush();
    }
    std::filesystem::rename(tempPath, finalPath);
  }

 private:
  std::filesystem::path pathFor(const std::string& sessionKey) const {
    std::string safe = sessionKey;
    for (char& c : safe) {
      if (c == '/' || c == '>' || c == '-') {
        c = '_';
      }
    }
    return std::filesystem::path(directory_) / (safe + ".seq");
  }

  std::string directory_;
  std::mutex mutex_;
  std::map<std::string, SequenceNumbers> memory_;
};

// One inbound FIX application message.
//
// respond() DELIBERATELY SENDS NOTHING. specification.md §8.11: on a
// session transport the synchronous receipt is bookkeeping only, and
// every output for the session -- including the ones the state machine
// designated -- is delivered by the output side from the journal, in
// sequence-number order. Returning the receipt here too would be the
// exactly-once violation the rule exists to prevent, and would let a
// synchronous copy overtake journal-ordered outputs on the same
// session, which is the cross-order ordering hazard §8.11 describes.
class FixRequestContext : public sequencer::RequestContext {
 public:
  FixRequestContext(std::string body, std::uint64_t sessionId)
      : body_(std::move(body)), sessionId_(sessionId) {}

  sequencer::Payload body() const override {
    return sequencer::Payload(reinterpret_cast<const std::byte*>(body_.data()), body_.size());
  }

  std::uint64_t session() const override { return sessionId_; }

  void respond(sequencer::Payload /*response*/) override {
    // Consumed, not sent. The chassis has already withheld the
    // designated outputs (§8.11 guard in request_pipeline.hpp), so
    // whatever the codec produced here is a receipt-shaped
    // acknowledgement with nothing in it for the client; the client's
    // answer is the execution report the output side will deliver.
    accepted_ = true;
  }

  void fail(const std::string& message) override {
    // A rejection has no journal output to arrive later, so this is the
    // one thing the input side must surface itself. The transport turns
    // it into a session-level Reject.
    rejected_ = true;
    failure_ = message;
  }

  bool rejected() const { return rejected_; }
  bool accepted() const { return accepted_; }
  const std::string& failure() const { return failure_; }

 private:
  std::string body_;
  std::uint64_t sessionId_;
  bool accepted_ = false;
  bool rejected_ = false;
  std::string failure_;
};

}  // namespace

// One accepted FIX connection: a socket, the session state machine
// driving it, and the thread that reads it.
struct FixConnection {
  explicit FixConnection(tcp::socket s) : socket(std::move(s)) {}

  tcp::socket socket;
  std::unique_ptr<FixSession> session;
  // Routing identity: which CONNECTION an output goes to. Distinct
  // from the FIX session identity below, which is the CompID pair and
  // survives reconnects -- conflating the two was the bug this pair of
  // fields exists to keep fixed.
  std::uint64_t sessionId = 0;
  std::string identityKey;
  // Serializes writes: the reader thread writes replies, and on a
  // session gateway the OUTPUT side writes execution reports on this
  // same session, from its own thread.
  std::mutex writeMutex;
  std::thread reader;
  std::atomic<bool> stop{false};
  std::atomic<bool> loggedOn{false};
};

struct FixInputTransport::Impl {
  explicit Impl(FixInputConfig cfg)
      : config(std::move(cfg)), sequences(config.sequenceStoreDir) {}

  FixInputConfig config;
  FileSequenceStore sequences;
  Authenticator authenticator = acceptAnyCredentials();

  RequestFn onRequest;
  DisconnectFn onDisconnect;

  net::io_context ioContext;
  std::unique_ptr<tcp::acceptor> acceptor;
  std::thread acceptThread;
  std::atomic<bool> stopping{false};

  std::mutex connectionsMutex;
  std::map<std::uint64_t, std::shared_ptr<FixConnection>> connections;
  std::atomic<std::uint64_t> nextSessionId{1};
  // CompID pairs currently held by a live connection.
  std::set<std::string> liveIdentities;
  // Kept so stop() can join them. InputTransport::stop()'s contract is
  // that no callback can still fire once it returns, and a detached
  // reader thread cannot promise that -- it would still be holding
  // `this` after the transport began destruction.
  std::vector<std::thread> readers;
  std::mutex readersMutex;

  void armAccept() {
    auto socket = std::make_shared<tcp::socket>(ioContext);
    acceptor->async_accept(*socket, [this, socket](const boost::system::error_code& ec) {
      if (ec) {
        return;  // acceptor closed, or shutting down
      }
      onAccepted(std::move(*socket));
      if (!stopping.load(std::memory_order_relaxed)) {
        armAccept();
      }
    });
  }

  void onAccepted(tcp::socket socket) {
    const std::uint64_t sessionId = this->nextSessionId.fetch_add(1);
    auto connection = std::make_shared<FixConnection>(std::move(socket));
    connection->sessionId = sessionId;

    SessionConfig sessionConfig;
    sessionConfig.role = Role::Acceptor;
    sessionConfig.senderCompId = this->config.senderCompId;
    // Left as configured -- EMPTY means "adopt from the Logon's
    // SenderCompID", which is how a reconnecting client resumes its own
    // session and its own sequence counters. Synthesizing a
    // per-connection CompID here (an earlier version did) made every
    // reconnect a brand-new session, which is exactly what a FIX client
    // must not experience.
    sessionConfig.targetCompId = this->config.targetCompId;
    sessionConfig.heartBtInt = this->config.heartBtInt;
    connection->session =
        std::make_unique<FixSession>(sessionConfig, this->sequences, steadyMicros);
    connection->session->setAuthenticator(this->authenticator);
    // Refuse a second live connection for an identity already in use.
    // Counters are keyed by CompID now, so two connections sharing one
    // identity would interleave writers onto a single pair.
    connection->session->setIdentityGuard([this, connection](const std::string& key) {
      std::lock_guard<std::mutex> lock(this->connectionsMutex);
      if (this->liveIdentities.count(key) != 0) {
        return false;
      }
      this->liveIdentities.insert(key);
      connection->identityKey = key;
      return true;
    });
    connection->session->setSendFn([this, connection](std::string_view frame) {
      this->write(*connection, frame);
    });
    connection->session->setAppMessageFn([this, connection](const hffix::message_reader& message) {
      if (!this->onRequest) {
        return;
      }
      // The transport never interprets an application message: it is
      // handed to the codec exactly as it arrived (§8.10).
      this->onRequest(std::make_shared<FixRequestContext>(
          std::string(message.message_begin(), message.message_size()), connection->sessionId));
    });
    connection->session->setEventFn(
        [this, connection](SessionEvent event, DisconnectReason reason) {
          if (event == SessionEvent::LogonComplete) {
            connection->loggedOn.store(true, std::memory_order_relaxed);
            return;
          }
          if (event != SessionEvent::Disconnected &&
              event != SessionEvent::LogoutComplete) {
            return;
          }
          // A FIX Logout is an explicit event, distinct from a socket
          // drop -- specification.md §8.1's onDisconnect contract
          // wants them told apart, and a state machine that proposes
          // a disconnect input may care which happened.
          if (connection->loggedOn.exchange(false, std::memory_order_relaxed) &&
              this->onDisconnect) {
            this->onDisconnect(sequencer::SessionInfo{connection->sessionId});
          }
          (void)reason;
          connection->stop.store(true, std::memory_order_relaxed);
        });

    {
      std::lock_guard<std::mutex> lock(this->connectionsMutex);
      this->connections[sessionId] = connection;
    }

    connection->session->start();
    connection->reader = std::thread([this, connection] {
      // A receive timeout rather than a second thread per session:
      // the read returns periodically so poll() can run the heartbeat
      // and peer-silence logic on the same thread that owns the
      // session's inbound path.
      struct timeval timeout {};
      timeout.tv_sec = 1;
      ::setsockopt(connection->socket.native_handle(), SOL_SOCKET, SO_RCVTIMEO, &timeout,
                   sizeof(timeout));

      std::vector<char> buffer(16 * 1024);
      while (!connection->stop.load(std::memory_order_relaxed) &&
             !this->stopping.load(std::memory_order_relaxed)) {
        boost::system::error_code ec;
        const std::size_t n =
            connection->socket.read_some(net::buffer(buffer.data(), buffer.size()), ec);
        if (ec) {
          if (ec == boost::asio::error::would_block || ec == boost::asio::error::try_again) {
            connection->session->poll();
            continue;
          }
          break;  // peer gone
        }
        connection->session->onBytes(std::string_view(buffer.data(), n));
        connection->session->poll();
      }

      // A socket drop with no FIX Logout: still a session loss.
      if (connection->loggedOn.exchange(false, std::memory_order_relaxed) &&
          this->onDisconnect) {
        this->onDisconnect(sequencer::SessionInfo{connection->sessionId});
      }
      std::lock_guard<std::mutex> lock(this->connectionsMutex);
      this->connections.erase(connection->sessionId);
    });
    {
      std::lock_guard<std::mutex> lock(readersMutex);
      readers.push_back(std::move(connection->reader));
    }
  }

  void write(FixConnection& connection, std::string_view frame) {
    std::lock_guard<std::mutex> lock(connection.writeMutex);
    boost::system::error_code ec;
    net::write(connection.socket, net::buffer(frame.data(), frame.size()), ec);
    // A write failure means the peer is gone; the reader thread will
    // see the same and run the disconnect path once.
    (void)ec;
  }
};


FixInputTransport::FixInputTransport(FixInputConfig config)
    : impl_(std::make_unique<Impl>(std::move(config))) {}

FixInputTransport::~FixInputTransport() { stop(); }

void FixInputTransport::attach(RequestFn onRequest, DisconnectFn onDisconnect) {
  impl_->onRequest = std::move(onRequest);
  impl_->onDisconnect = std::move(onDisconnect);
}

void FixInputTransport::setAuthenticator(Authenticator authenticator) {
  impl_->authenticator = std::move(authenticator);
}

FixSession* FixInputTransport::sessionFor(std::uint64_t sessionId) {
  std::lock_guard<std::mutex> lock(impl_->connectionsMutex);
  const auto it = impl_->connections.find(sessionId);
  return it == impl_->connections.end() ? nullptr : it->second->session.get();
}

void FixInputTransport::start(int listenPort) {
  impl_->acceptor = std::make_unique<tcp::acceptor>(
      impl_->ioContext, tcp::endpoint(tcp::v4(), static_cast<unsigned short>(listenPort)));

  // async_accept, not a blocking accept() on its own thread. A thread
  // parked inside Asio's SYNCHRONOUS accept is not reliably woken by
  // closing the acceptor from another thread, so stop() deadlocked on
  // the join -- found the first time this transport was tested. With an
  // async accept the accept thread is just an io_context runner, and
  // stopping the context ends it deterministically.
  impl_->armAccept();
  impl_->acceptThread = std::thread([this] { impl_->ioContext.run(); });
}

void FixInputTransport::stop() {
  if (impl_ == nullptr || impl_->stopping.exchange(true)) {
    return;
  }
  if (impl_->acceptor != nullptr) {
    boost::system::error_code ec;
    impl_->acceptor->close(ec);
  }
  impl_->ioContext.stop();
  if (impl_->acceptThread.joinable()) {
    impl_->acceptThread.join();
  }

  // Close every live socket so its reader's blocking read returns.
  std::vector<std::shared_ptr<FixConnection>> live;
  {
    std::lock_guard<std::mutex> lock(impl_->connectionsMutex);
    for (auto& [id, connection] : impl_->connections) {
      live.push_back(connection);
    }
  }
  for (auto& connection : live) {
    connection->stop.store(true, std::memory_order_relaxed);
    boost::system::error_code ec;
    connection->socket.close(ec);
  }

  // Then JOIN them: InputTransport::stop() promises no callback can
  // still fire once it returns, which a sleep cannot deliver.
  std::vector<std::thread> readers;
  {
    std::lock_guard<std::mutex> lock(impl_->readersMutex);
    readers.swap(impl_->readers);
  }
  for (std::thread& reader : readers) {
    if (reader.joinable()) {
      reader.join();
    }
  }
}

}  // namespace sequencer::fix
