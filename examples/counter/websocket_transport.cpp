#include "websocket_transport.hpp"

#include <boost/asio/dispatch.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/post.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/websocket.hpp>

#include <deque>
#include <future>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

// Boost.Beast's websocket::stream has one hard rule (its own docs,
// verbatim): "Shared objects: Unsafe." Every operation on one
// connection's stream — the read that exists solely to detect the
// client closing, and every write triggered by Fanout::broadcast/
// toSession from the output gateway's own tailing thread — must
// therefore run on the same thread. The design here is the standard
// Beast answer to that: one io_context, one thread running it, and
// broadcast()/toSession() never touch a stream directly — they
// boost::asio::post the actual write onto that thread instead.

namespace beast = boost::beast;
namespace websocket = beast::websocket;
namespace net = boost::asio;
using tcp = net::ip::tcp;

namespace sequencer::examples::counter {

namespace {

class Connection : public std::enable_shared_from_this<Connection> {
 public:
  Connection(tcp::socket socket, sequencer::SessionId sessionId, WebSocketTransport::Impl& transport)
      : ws_(std::move(socket)), sessionId_(sessionId), transport_(transport) {}

  sequencer::SessionId sessionId() const { return sessionId_; }

  void start() {
    ws_.async_accept([self = shared_from_this()](beast::error_code ec) { self->onAccept(ec); });
  }

  // Only ever called on the io_context's own thread (via net::post),
  // so no synchronization is needed for writeQueue_/writing_ here.
  void enqueueWrite(std::shared_ptr<std::string> message) {
    writeQueue_.push_back(std::move(message));
    if (!writing_) {
      writeNext();
    }
  }

  void requestClose() {
    boost::system::error_code ec;
    ws_.next_layer().close(ec);
  }

 private:
  void onAccept(beast::error_code ec);
  void doRead();
  void onRead(beast::error_code ec);
  void writeNext();

  websocket::stream<tcp::socket> ws_;
  beast::flat_buffer buffer_;
  sequencer::SessionId sessionId_;
  WebSocketTransport::Impl& transport_;
  std::deque<std::shared_ptr<std::string>> writeQueue_;
  bool writing_ = false;
};

}  // namespace

struct WebSocketTransport::Impl {
  net::io_context ioContext;
  std::unique_ptr<tcp::acceptor> acceptor;
  std::thread ioThread;
  bool stopping = false;

  std::mutex registryMutex;
  std::unordered_map<sequencer::SessionId, std::shared_ptr<Connection>> sessionToConnection;
  std::unordered_map<std::string, std::unordered_set<sequencer::SessionId>> topicToSessions;
  std::atomic<sequencer::SessionId> nextSessionId{1};

  // counter has exactly one implicit broadcast topic — every connected
  // client joins it, matching CounterOutputCodec's own "totals" topic.
  static constexpr const char* kTopic = "totals";

  void doAccept() {
    acceptor->async_accept([this](beast::error_code ec, tcp::socket socket) {
      if (!ec) {
        const sequencer::SessionId sessionId = nextSessionId.fetch_add(1, std::memory_order_relaxed);
        auto conn = std::make_shared<Connection>(std::move(socket), sessionId, *this);
        conn->start();
      }
      if (!stopping) {
        doAccept();
      }
    });
  }

  void registerConnection(const std::shared_ptr<Connection>& conn) {
    std::lock_guard<std::mutex> lock(registryMutex);
    sessionToConnection[conn->sessionId()] = conn;
    topicToSessions[kTopic].insert(conn->sessionId());
  }

  void deregisterConnection(sequencer::SessionId sessionId) {
    std::lock_guard<std::mutex> lock(registryMutex);
    sessionToConnection.erase(sessionId);
    for (auto& [topic, sessions] : topicToSessions) {
      sessions.erase(sessionId);
    }
  }

  void postWrite(sequencer::SessionId sessionId, std::shared_ptr<std::string> message) {
    std::shared_ptr<Connection> conn;
    {
      std::lock_guard<std::mutex> lock(registryMutex);
      const auto it = sessionToConnection.find(sessionId);
      if (it == sessionToConnection.end()) {
        return;
      }
      conn = it->second;
    }
    net::post(ioContext, [conn, message] { conn->enqueueWrite(message); });
  }
};

void Connection::onAccept(beast::error_code ec) {
  if (ec) {
    return;  // handshake failed; nothing was registered, nothing to undo
  }
  transport_.registerConnection(shared_from_this());
  doRead();
}

void Connection::doRead() {
  ws_.async_read(buffer_, [self = shared_from_this()](beast::error_code ec, std::size_t) {
    self->onRead(ec);
  });
}

void Connection::onRead(beast::error_code ec) {
  if (ec) {
    // Closed, reset, or (during shutdown) the socket was force-closed —
    // either way, this connection is done.
    transport_.deregisterConnection(sessionId_);
    return;
  }
  buffer_.consume(buffer_.size());
  doRead();
}

void Connection::writeNext() {
  if (writeQueue_.empty()) {
    writing_ = false;
    return;
  }
  writing_ = true;
  ws_.text(true);
  ws_.async_write(net::buffer(*writeQueue_.front()),
                  [self = shared_from_this()](beast::error_code /*ec*/, std::size_t) {
                    // Errors are swallowed here — a dead connection's
                    // read loop will notice and deregister it; delivery
                    // is best-effort, matching Fanout's documented
                    // contract.
                    self->writeQueue_.pop_front();
                    self->writeNext();
                  });
}

WebSocketTransport::WebSocketTransport() : impl_(std::make_unique<Impl>()) {}
WebSocketTransport::~WebSocketTransport() = default;

void WebSocketTransport::start(int listenPort) {
  impl_->acceptor = std::make_unique<tcp::acceptor>(
      impl_->ioContext, tcp::endpoint(tcp::v4(), static_cast<unsigned short>(listenPort)));
  impl_->doAccept();
  impl_->ioThread = std::thread([this] { impl_->ioContext.run(); });
}

void WebSocketTransport::stop() {
  // Everything that touches acceptor_/a connection's stream must run on
  // the io thread — including telling it to stop accepting and to close
  // every live connection. Block until that has actually happened
  // before stopping the io_context out from under it.
  std::promise<void> closedPromise;
  net::post(impl_->ioContext, [this, &closedPromise] {
    impl_->stopping = true;
    boost::system::error_code ec;
    impl_->acceptor->close(ec);
    std::vector<std::shared_ptr<Connection>> connections;
    {
      std::lock_guard<std::mutex> lock(impl_->registryMutex);
      connections.reserve(impl_->sessionToConnection.size());
      for (auto& [sessionId, conn] : impl_->sessionToConnection) {
        connections.push_back(conn);
      }
    }
    for (auto& conn : connections) {
      conn->requestClose();
    }
    closedPromise.set_value();
  });
  closedPromise.get_future().wait();

  impl_->ioContext.stop();
  if (impl_->ioThread.joinable()) {
    impl_->ioThread.join();
  }
}

void WebSocketTransport::toSession(sequencer::SessionId owner, Bytes bytes) {
  auto message = std::make_shared<std::string>(reinterpret_cast<const char*>(bytes.data()), bytes.size());
  impl_->postWrite(owner, std::move(message));
}

void WebSocketTransport::broadcast(const std::string& topic, Bytes bytes) {
  auto message = std::make_shared<std::string>(reinterpret_cast<const char*>(bytes.data()), bytes.size());
  std::vector<sequencer::SessionId> targets;
  {
    std::lock_guard<std::mutex> lock(impl_->registryMutex);
    const auto it = impl_->topicToSessions.find(topic);
    if (it == impl_->topicToSessions.end()) {
      return;
    }
    targets.assign(it->second.begin(), it->second.end());
  }
  for (sequencer::SessionId sessionId : targets) {
    impl_->postWrite(sessionId, message);
  }
}

}  // namespace sequencer::examples::counter
