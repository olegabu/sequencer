#include <sequencer/websocket_output_transport.hpp>

#include "output_batch_metrics.hpp"

#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/post.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/websocket.hpp>

#include <atomic>
#include <cstdint>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

// Boost.Beast's websocket::stream has one hard rule (its own docs,
// verbatim): "Shared objects: Unsafe" — every operation on one
// connection's stream must come from one thread at a time. The
// previous design satisfied that by funneling ALL streams' operations
// onto one shared io_context thread via net::post — a cross-thread
// hand-off per delivery, which is exactly the latency the
// BroadcastRing redesign removes (include/sequencer/broadcast_ring.hpp's
// file comment). This design satisfies the same rule by OWNERSHIP
// instead: the io thread's only jobs are accepting connections and
// running the HTTP upgrade handshake; the moment a connection is
// established it is handed off wholesale to a dedicated writer thread,
// which from then on is the only thread that ever touches the stream —
// draining the ring through its own private cursor and calling Beast's
// synchronous write() directly. No posts, no shared write queue.
//
// Subscribers never send data, so there is no read loop at all: a
// departed client surfaces as an error on the next write. stop()
// unblocks any writer stuck inside a blocking write() by closing the
// underlying socket from outside — the same idiom
// WebSocketOutputObserver::stop() (bench/load_generator) already uses
// on its own blocking read(), and the one deliberate exception to
// strict single-thread stream access (a raw socket close, not a
// websocket operation).
//
// Wire format: one **binary** frame per drained batch, each payload
// framed by a 4-byte big-endian length prefix — websocket is
// message-oriented like brpc's Stream (one write is one frame), so a
// raw concatenation would arrive as one corrupted frame; the length
// prefix is the minimal framing that keeps an OutputCodec's bytes
// otherwise completely unmodified (same reasoning as
// brpc_stream_fanout.hpp's). Decoded by websocket_output_transport_test's
// client, the counter example's e2e test client, and
// bench/load_generator's WebSocket observer.
//
// Binary, not text, and this is not cosmetic. RFC 6455 requires a text
// frame's payload to be valid UTF-8, and Beast enforces it: a write
// that violates it fails, and since a fanout write is best-effort the
// payload is then dropped with nothing logged. Two things here are
// routinely not valid UTF-8. The length prefix itself: any payload of
// 128 bytes or more puts a byte >= 0x80 in it, which is a UTF-8 lead
// byte with no valid continuation. And the payloads, which are
// whatever an OutputCodec emits — this transport's whole contract is
// carrying those bytes unmodified, and nothing says they are text.
// Sending text frames silently broke both cases; a chassis test
// serving one journal to a brpc and a WebSocket subscriber at once
// caught it, because the brpc subscriber received records the
// WebSocket one never did. Consumers see Blob/ArrayBuffer in a
// browser rather than a string, which is the correct shape for a
// length-prefixed batch anyway.

namespace beast = boost::beast;
namespace http = beast::http;
namespace websocket = beast::websocket;
namespace net = boost::asio;
using tcp = net::ip::tcp;

namespace sequencer {

namespace {

// One accepted-and-upgraded connection, owned by its writer thread.
struct Session {
  explicit Session(websocket::stream<tcp::socket> ws) : ws(std::move(ws)) {}
  websocket::stream<tcp::socket> ws;
  std::thread writer;
  std::atomic<bool> stop{false};
};

// The pre-handshake half: reads the HTTP upgrade request on the io
// thread (the request target/URL path is where a client's topic comes
// from — see this transport's header comment), accepts, then hands
// the finished stream to the transport to spawn its writer.
class PendingConnection : public std::enable_shared_from_this<PendingConnection> {
 public:
  PendingConnection(tcp::socket socket, WebSocketOutputTransport::Impl& transport)
      : ws_(std::move(socket)), transport_(transport) {}

  void start() {
    http::async_read(ws_.next_layer(), buffer_, request_,
                      [self = shared_from_this()](beast::error_code ec, std::size_t) { self->onReadRequest(ec); });
  }

 private:
  void onReadRequest(beast::error_code ec);
  void onAccept(beast::error_code ec);

  websocket::stream<tcp::socket> ws_;
  beast::flat_buffer buffer_;
  http::request<http::string_body> request_;
  WebSocketOutputTransport::Impl& transport_;
};

}  // namespace

struct WebSocketOutputTransport::Impl {
  net::io_context ioContext;
  std::unique_ptr<tcp::acceptor> acceptor;
  std::thread ioThread;
  bool stopping = false;

  BroadcastRing* ring = nullptr;
  TopicRegistry* topics = nullptr;
  int idleSpinIterations = 1000;

  std::mutex sessionsMutex;
  std::unordered_map<std::uint64_t, std::shared_ptr<Session>> sessions;
  std::atomic<std::uint64_t> nextSessionId{1};

  void doAccept() {
    acceptor->async_accept([this](beast::error_code ec, tcp::socket socket) {
      if (!ec) {
        // Nagle off, for the same reason the FIX transports set it: a
        // small write that waits on the peer's delayed ACK stalls up to
        // 40ms. brpc and gRPC set this in their own socket layers;
        // Beast does not, so this transport is the one output flavour
        // that was still Nagled -- and it is the one whose p90 leaves
        // the other two above 150k (2,182-2,506us against ~1,630us),
        // which was previously attributed to its per-connection writer
        // thread without checking this.
        boost::system::error_code nodelayEc;
        socket.set_option(tcp::no_delay(true), nodelayEc);
        std::make_shared<PendingConnection>(std::move(socket), *this)->start();
      }
      if (!stopping) {
        doAccept();
      }
    });
  }

  // Called on the io thread once the websocket handshake completed.
  // From here on the new writer thread owns the stream exclusively.
  void adopt(websocket::stream<tcp::socket> ws, const std::string& topic) {
    const std::uint64_t sessionId = nextSessionId.fetch_add(1, std::memory_order_relaxed);
    auto session = std::make_shared<Session>(std::move(ws));
    const std::uint64_t topicTag = makeTopicTag(topics->idFor(topic));
    const std::uint64_t sessionTag = makeSessionTag(sessionId);
    // Cursor captured here, on the registering thread, NOT inside the
    // writer thread: a freshly-spawned thread can be scheduled
    // arbitrarily late, and a head() read that happens only then
    // silently skips everything published in between as
    // pre-subscription history (a real, reproduced flake in the brpc
    // transport's identical path — see brpc_stream_fanout.hpp).
    const std::uint64_t initialCursor = ring->head();
    // Registration and the thread-object assignment both happen under
    // sessionsMutex: the writer's self-deregistration tail (below)
    // takes the same mutex before touching session->writer, which
    // orders its detach() strictly after this assignment completes
    // even if writeLoop exits immediately.
    std::lock_guard<std::mutex> lock(sessionsMutex);
    if (stopping) {
      return;  // raced shutdown; the socket just closes with the Session
    }
    sessions[sessionId] = session;
    session->writer = std::thread([this, sessionId, topicTag, sessionTag, initialCursor, session] {
      writeLoop(topicTag, sessionTag, initialCursor, *session);
      // Self-deregistration on the way out (client gone, or overrun):
      // drop the map's reference so the Session is destroyed once this
      // thread finishes with it. stop() may have taken it already —
      // then the entry is gone and stop() joins this thread instead.
      std::lock_guard<std::mutex> tailLock(sessionsMutex);
      const auto it = sessions.find(sessionId);
      if (it != sessions.end() && it->second == session) {
        it->second->writer.detach();  // this very thread; join would deadlock
        sessions.erase(it);
      }
    });
  }

  // One subscriber's whole delivery path (see brpc_stream_fanout.hpp's
  // readLoop — same shape): drain, filter by tag, one frame per
  // drained batch, spin-then-back-off when caught up, disconnect on
  // overrun.
  void writeLoop(std::uint64_t topicTag, std::uint64_t sessionTag, std::uint64_t initialCursor,
                 Session& session) {
    std::uint64_t cursor = initialCursor;
    std::vector<std::byte> payload(ring->maxPayload());
    IdleStrategy idle(idleSpinIterations);
    constexpr int kMaxBatch = 1024;
    std::string frame;
    session.ws.binary(true);
    while (!session.stop.load(std::memory_order_relaxed)) {
      frame.clear();
      int gathered = 0;
      bool overrun = false;
      while (gathered < kMaxBatch) {
        std::uint64_t tag = 0;
        std::uint32_t length = 0;
        const auto result = ring->readOne(cursor, tag, payload.data(), length);
        if (result == BroadcastRing::ReadResult::Empty) {
          break;
        }
        if (result == BroadcastRing::ReadResult::Overrun) {
          overrun = true;
          break;
        }
        if (tag != topicTag && tag != sessionTag) {
          continue;  // someone else's entry; not counted against the batch cap
        }
        frame.push_back(static_cast<char>(length >> 24));
        frame.push_back(static_cast<char>(length >> 16));
        frame.push_back(static_cast<char>(length >> 8));
        frame.push_back(static_cast<char>(length));
        frame.append(reinterpret_cast<const char*>(payload.data()), length);
        ++gathered;
      }
      if (!frame.empty()) {
        boost::system::error_code ec;
        session.ws.write(net::buffer(frame), ec);
        if (ec) {
          return;  // client gone (or stop() closed the socket under us)
        }
        gateway::output::detail::websocketBatchMetrics().recordBatch(gathered);
        idle.reset();
      }
      if (overrun) {
        boost::system::error_code ec;
        session.ws.next_layer().close(ec);  // broadcast_ring.hpp's slow-consumer contract
        return;
      }
      if (frame.empty()) {
        idle.idle();
      }
    }
  }
};

void PendingConnection::onReadRequest(beast::error_code ec) {
  if (ec) {
    return;  // client disconnected before finishing the handshake
  }
  ws_.async_accept(request_, [self = shared_from_this()](beast::error_code ec) { self->onAccept(ec); });
}

void PendingConnection::onAccept(beast::error_code ec) {
  if (ec) {
    return;  // handshake failed; nothing was registered, nothing to undo
  }
  std::string topic(request_.target());
  if (!topic.empty() && topic.front() == '/') {
    topic.erase(0, 1);
  }
  transport_.adopt(std::move(ws_), topic);
}

WebSocketOutputTransport::WebSocketOutputTransport() : impl_(std::make_unique<Impl>()) {}
WebSocketOutputTransport::~WebSocketOutputTransport() = default;

void WebSocketOutputTransport::attach(BroadcastRing& ring, TopicRegistry& topics, int idleSpinIterations) {
  impl_->ring = &ring;
  impl_->topics = &topics;
  impl_->idleSpinIterations = idleSpinIterations;
}

void WebSocketOutputTransport::start(int listenPort) {
  impl_->acceptor = std::make_unique<tcp::acceptor>(
      impl_->ioContext, tcp::endpoint(tcp::v4(), static_cast<unsigned short>(listenPort)));
  impl_->doAccept();
  impl_->ioThread = std::thread([this] { impl_->ioContext.run(); });
}

void WebSocketOutputTransport::stop() {
  // Stop accepting first — closing the acceptor must happen on the io
  // thread (it owns it), and blocking until that lands keeps a new
  // connection from slipping in mid-shutdown.
  std::promise<void> acceptorClosed;
  net::post(impl_->ioContext, [this, &acceptorClosed] {
    impl_->stopping = true;
    if (impl_->acceptor) {
      boost::system::error_code ec;
      impl_->acceptor->close(ec);
    }
    acceptorClosed.set_value();
  });
  acceptorClosed.get_future().wait();

  // Take every live session, then close each one's socket from
  // outside to unblock any writer stuck in a blocking write(), and
  // join. (Also mark stopping under sessionsMutex so adopt() can't
  // add a session after this snapshot.)
  std::vector<std::shared_ptr<Session>> sessions;
  {
    std::lock_guard<std::mutex> lock(impl_->sessionsMutex);
    impl_->stopping = true;
    sessions.reserve(impl_->sessions.size());
    for (auto& [sessionId, session] : impl_->sessions) {
      (void)sessionId;
      sessions.push_back(session);
    }
    impl_->sessions.clear();
  }
  for (auto& session : sessions) {
    session->stop.store(true, std::memory_order_relaxed);
    boost::system::error_code ec;
    session->ws.next_layer().shutdown(tcp::socket::shutdown_both, ec);
    session->ws.next_layer().close(ec);
  }
  for (auto& session : sessions) {
    if (session->writer.joinable()) {
      session->writer.join();
    }
  }

  impl_->ioContext.stop();
  if (impl_->ioThread.joinable()) {
    impl_->ioThread.join();
  }
}

}  // namespace sequencer
