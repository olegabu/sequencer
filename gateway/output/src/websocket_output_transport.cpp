#include <sequencer/websocket_output_transport.hpp>

#include <boost/asio/dispatch.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/post.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/websocket.hpp>

#include <cstdint>
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
//
// toSession()/broadcast() accumulate into a per-session pending buffer
// rather than posting immediately — flush() is what actually posts,
// once per session with anything pending, batching whatever arrived
// since the last flush into one net::post() + one websocket text
// frame instead of one of each per payload. Same reasoning as
// StreamFanout's own append()/flush() (gateway/output/src/
// stream_fanout.hpp, brpc's transport) and the same framing choice:
// websocket is message-oriented like brpc's own Stream (one
// async_write() call is one frame, not raw bytes a receiver must frame
// itself), so a plain concatenation of several payloads would produce
// one corrupted, unparseable frame instead of several correct ones —
// each accumulated payload gets its own 4-byte big-endian length
// prefix, not a protobuf envelope, for the same reason StreamFanout's
// own comment gives: this transport's whole point is carrying an
// OutputCodec's bytes completely unmodified.

namespace beast = boost::beast;
namespace http = beast::http;
namespace websocket = beast::websocket;
namespace net = boost::asio;
using tcp = net::ip::tcp;

namespace sequencer {

namespace {

class Connection : public std::enable_shared_from_this<Connection> {
 public:
  Connection(tcp::socket socket, SessionId sessionId, WebSocketOutputTransport::Impl& transport)
      : ws_(std::move(socket)), sessionId_(sessionId), transport_(transport) {}

  SessionId sessionId() const { return sessionId_; }

  void start() {
    // Read the HTTP upgrade request ourselves, rather than the
    // one-step ws_.async_accept(), so the request target (URL path) is
    // available first — that's where a connecting client's topic
    // comes from (see this header's file comment).
    http::async_read(ws_.next_layer(), buffer_, request_,
                      [self = shared_from_this()](beast::error_code ec, std::size_t) { self->onReadRequest(ec); });
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
  void onReadRequest(beast::error_code ec);
  void onAccept(beast::error_code ec);
  void doRead();
  void onRead(beast::error_code ec);
  void writeNext();

  websocket::stream<tcp::socket> ws_;
  beast::flat_buffer buffer_;
  http::request<http::string_body> request_;
  SessionId sessionId_;
  WebSocketOutputTransport::Impl& transport_;
  std::deque<std::shared_ptr<std::string>> writeQueue_;
  bool writing_ = false;
};

}  // namespace

struct WebSocketOutputTransport::Impl {
  net::io_context ioContext;
  std::unique_ptr<tcp::acceptor> acceptor;
  std::thread ioThread;
  bool stopping = false;

  std::mutex registryMutex;
  std::unordered_map<SessionId, std::shared_ptr<Connection>> sessionToConnection;
  std::unordered_map<std::string, std::unordered_set<SessionId>> topicToSessions;
  std::unordered_map<SessionId, std::string> pending;  // length-prefixed, accumulated by appendPending(), sent by flush()
  std::atomic<SessionId> nextSessionId{1};

  void doAccept() {
    acceptor->async_accept([this](beast::error_code ec, tcp::socket socket) {
      if (!ec) {
        const SessionId sessionId = nextSessionId.fetch_add(1, std::memory_order_relaxed);
        auto conn = std::make_shared<Connection>(std::move(socket), sessionId, *this);
        conn->start();
      }
      if (!stopping) {
        doAccept();
      }
    });
  }

  void registerConnection(const std::shared_ptr<Connection>& conn, const std::string& topic) {
    std::lock_guard<std::mutex> lock(registryMutex);
    sessionToConnection[conn->sessionId()] = conn;
    topicToSessions[topic].insert(conn->sessionId());
  }

  void deregisterConnection(SessionId sessionId) {
    std::lock_guard<std::mutex> lock(registryMutex);
    sessionToConnection.erase(sessionId);
    for (auto& [topic, sessions] : topicToSessions) {
      sessions.erase(sessionId);
    }
    // A disconnected session's own unflushed bytes (if flush() hasn't
    // run since its last appendPending()) can never be sent — drop
    // them rather than let them accumulate in pending forever.
    pending.erase(sessionId);
  }

  void postWrite(SessionId sessionId, std::shared_ptr<std::string> message) {
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

  // Appends this payload's length-prefixed frame to sessionId's
  // pending buffer. Caller must hold registryMutex. A no-op if the
  // session isn't currently connected, matching postWrite()'s own
  // best-effort semantics — otherwise a delivery aimed at a session
  // that never (re)connects would accumulate in pending forever.
  void appendPending(SessionId sessionId, const std::shared_ptr<std::string>& message) {
    if (sessionToConnection.find(sessionId) == sessionToConnection.end()) {
      return;
    }
    std::string& buf = pending[sessionId];
    const auto length = static_cast<std::uint32_t>(message->size());
    buf.push_back(static_cast<char>(length >> 24));
    buf.push_back(static_cast<char>(length >> 16));
    buf.push_back(static_cast<char>(length >> 8));
    buf.push_back(static_cast<char>(length));
    buf.append(*message);
  }

  // Sends everything accumulated by appendPending() since the last
  // flush(), one net::post()/websocket text frame per session that
  // actually has pending data — not one per toSession()/broadcast()
  // call. See this file's own top comment for the numbers this is
  // modeled on (StreamFanout's own brpc-specific measurement).
  void flush() {
    std::unordered_map<SessionId, std::string> toSend;
    {
      std::lock_guard<std::mutex> lock(registryMutex);
      if (pending.empty()) {
        return;
      }
      toSend.swap(pending);
    }
    for (auto& [sessionId, buf] : toSend) {
      postWrite(sessionId, std::make_shared<std::string>(std::move(buf)));
    }
  }
};

void Connection::onReadRequest(beast::error_code ec) {
  if (ec) {
    return;  // client disconnected before finishing the handshake
  }
  ws_.async_accept(request_, [self = shared_from_this()](beast::error_code ec) { self->onAccept(ec); });
}

void Connection::onAccept(beast::error_code ec) {
  if (ec) {
    return;  // handshake failed; nothing was registered, nothing to undo
  }
  std::string topic(request_.target());
  if (!topic.empty() && topic.front() == '/') {
    topic.erase(0, 1);
  }
  transport_.registerConnection(shared_from_this(), topic);
  doRead();
}

void Connection::doRead() {
  ws_.async_read(buffer_, [self = shared_from_this()](beast::error_code ec, std::size_t) { self->onRead(ec); });
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

WebSocketOutputTransport::WebSocketOutputTransport() : impl_(std::make_unique<Impl>()) {}
WebSocketOutputTransport::~WebSocketOutputTransport() = default;

void WebSocketOutputTransport::start(int listenPort) {
  impl_->acceptor = std::make_unique<tcp::acceptor>(
      impl_->ioContext, tcp::endpoint(tcp::v4(), static_cast<unsigned short>(listenPort)));
  impl_->doAccept();
  impl_->ioThread = std::thread([this] { impl_->ioContext.run(); });
}

void WebSocketOutputTransport::stop() {
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

void WebSocketOutputTransport::toSession(SessionId owner, Bytes bytes) {
  auto message = std::make_shared<std::string>(reinterpret_cast<const char*>(bytes.data()), bytes.size());
  std::lock_guard<std::mutex> lock(impl_->registryMutex);
  impl_->appendPending(owner, message);
}

void WebSocketOutputTransport::broadcast(const std::string& topic, Bytes bytes) {
  auto message = std::make_shared<std::string>(reinterpret_cast<const char*>(bytes.data()), bytes.size());
  std::lock_guard<std::mutex> lock(impl_->registryMutex);
  const auto it = impl_->topicToSessions.find(topic);
  if (it == impl_->topicToSessions.end()) {
    return;
  }
  for (SessionId sessionId : it->second) {
    impl_->appendPending(sessionId, message);
  }
}

void WebSocketOutputTransport::flush() { impl_->flush(); }

}  // namespace sequencer
