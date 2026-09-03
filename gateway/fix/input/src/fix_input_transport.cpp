#include <sequencer/fix/fix_input_transport.hpp>

#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/write.hpp>

#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstdlib>
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

// Reads MsgType without allocating.
bool msgTypeIs(const hffix::message_reader& message, std::string_view expected) {
  const auto type = message.message_type();
  if (type == message.end()) {
    return false;
  }
  return std::string_view(type->value().begin(), type->value().size()) == expected;
}

// Every Symbol (tag 55) in a MarketDataRequest -- one request may name
// several instruments, and each becomes a topic subscription.
std::vector<std::string> symbolsOf(const hffix::message_reader& message) {
  std::vector<std::string> symbols;
  for (auto it = message.begin(); it != message.end(); ++it) {
    if (it->tag() == 55) {
      symbols.emplace_back(it->value().begin(), it->value().size());
    }
  }
  return symbols;
}

// SubscriptionRequestType (263) decides which of the three requests a
// MarketDataRequest is. FIX 4.4 makes the tag required; a message
// without it is malformed, and we treat that as a snapshot -- the
// reading that registers NO standing subscription, so a malformed
// request cannot silently sign a session up for a permanent feed.
sequencer::fix::SessionSource::SubscriptionAction subscriptionActionOf(
    const hffix::message_reader& message) {
  using Action = sequencer::fix::SessionSource::SubscriptionAction;
  for (auto it = message.begin(); it != message.end(); ++it) {
    if (it->tag() == 263) {
      const std::string_view value(it->value().begin(), it->value().size());
      if (value == "1") {
        return Action::Subscribe;
      }
      if (value == "2") {
        return Action::Unsubscribe;
      }
      return Action::Snapshot;
    }
  }
  return Action::Snapshot;
}

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
    if (in >> numbers.nextOutbound >> numbers.nextInbound >> numbers.lastJournalSequence) {
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
    // Never throws out of here. This runs on a session's reader thread,
    // deep inside message handling, and an escaping exception takes the
    // whole gateway down -- which a full disk or a vanished directory
    // should not do.
    //
    // A lost counter write is not harmless: the session may resume at a
    // stale sequence number and need an operator reset. But that is
    // recoverable and affects one session, whereas terminating the
    // process drops every session on the gateway. Losing one is
    // strictly better than losing all of them.
    try {
      {
        std::ofstream out(tempPath, std::ios::trunc);
        out << numbers.nextOutbound << ' ' << numbers.nextInbound << ' '
            << numbers.lastJournalSequence << '\n';
        out.flush();
      }
      std::filesystem::rename(tempPath, finalPath);
    } catch (const std::exception&) {
      // Deliberately swallowed; see above. Worth surfacing through a
      // counter or log once this component has either.
    }
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

// Propose-receipt timing (FIX_STAGE_TIMERS).
//
// This is the measurement that separates the two costs in a session
// transport's latency. The receipt arrives when consensus has committed
// the record and the state machine has applied it -- exactly the moment
// a RequestResponse gateway would have answered the client. Everything
// after it is what §8.11's journal-ordered delivery adds on top.
//
// Both halves are timed in the same process, on the same run, against
// the same clients, so the comparison carries no cross-topology
// assumption: it is one clock, one host, one load.
std::atomic<std::uint64_t> g_proposeNs{0};
std::atomic<std::uint64_t> g_proposeCount{0};

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
  using InlineFn = std::function<void(std::uint64_t, std::string_view, std::uint64_t,
                                      std::uint32_t)>;

  FixRequestContext(std::string body, std::uint64_t sessionId, InlineFn inlineResponse)
      : body_(std::move(body)),
        sessionId_(sessionId),
        inlineResponse_(std::move(inlineResponse)),
        submittedAt_(std::chrono::steady_clock::now()) {}

  sequencer::Payload body() const override {
    return sequencer::Payload(reinterpret_cast<const std::byte*>(body_.data()), body_.size());
  }

  std::uint64_t session() const override { return sessionId_; }

  void noteReceipt(const sequencer::Receipt& receipt, std::size_t designatedOutputs) override {
    journalSequenceNumber_ = receipt.sequenceNumber;
    // The codec collapses however many designated outputs there were
    // into one message, so the whole span is accounted for by marking
    // the LAST index delivered -- marking only index 0 would let the
    // journal copy of a second output through as a duplicate.
    lastOutputIndex_ =
        designatedOutputs > 0 ? static_cast<std::uint32_t>(designatedOutputs - 1) : 0;
  }

  void respond(sequencer::Payload response) override {
    // Consumed, not sent. The chassis has already withheld the
    // designated outputs (§8.11 guard in request_pipeline.hpp), so
    // whatever the codec produced here is a receipt-shaped
    // acknowledgement with nothing in it for the client; the client's
    // answer is the execution report the output side will deliver.
    accepted_ = true;
    recordProposeLatency();
    if (inlineResponse_ && response.size() > 0) {
      inlineResponse_(sessionId_,
                      std::string_view(reinterpret_cast<const char*>(response.data()),
                                       response.size()),
                      journalSequenceNumber_, lastOutputIndex_);
    }
  }

  void fail(const std::string& message) override {
    // A rejection has no journal output to arrive later, so this is the
    // one thing the input side must surface itself. The transport turns
    // it into a session-level Reject.
    rejected_ = true;
    failure_ = message;
    recordProposeLatency();
  }

  bool rejected() const { return rejected_; }
  bool accepted() const { return accepted_; }
  const std::string& failure() const { return failure_; }

 private:
  std::string body_;
  void recordProposeLatency() {
    static const bool timed = std::getenv("FIX_STAGE_TIMERS") != nullptr;
    if (!timed) {
      return;
    }
    g_proposeNs.fetch_add(static_cast<std::uint64_t>(
                              std::chrono::duration_cast<std::chrono::nanoseconds>(
                                  std::chrono::steady_clock::now() - submittedAt_)
                                  .count()),
                          std::memory_order_relaxed);
    g_proposeCount.fetch_add(1, std::memory_order_relaxed);
  }

  std::uint64_t sessionId_;
  InlineFn inlineResponse_;
  std::uint64_t journalSequenceNumber_ = 0;
  std::uint32_t lastOutputIndex_ = 0;
  std::chrono::steady_clock::time_point submittedAt_;
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
  // Outbound coalescing buffer. Non-empty only between beginBatch() and
  // endBatch(); see SessionSource for why session-level traffic bypasses
  // it.
  std::string outBuffer;
  int batchDepth = 0;
  std::thread reader;
  std::atomic<bool> stop{false};
  std::atomic<bool> loggedOn{false};
};

// A session allowed to fall this far behind is dropped rather than
// buffered without limit.
constexpr std::size_t kMaxPendingBytes = 8u * 1024u * 1024u;

struct FixInputTransport::Impl {
  explicit Impl(FixInputConfig cfg)
      : config(std::move(cfg)), sequences(config.sequenceStoreDir) {}

  FixInputConfig config;
  FileSequenceStore sequences;
  Authenticator authenticator = acceptAnyCredentials();
  // Empty unless the gateway opted into inline answering.
  FixInputTransport::InlineResponseFn inlineResponse;

  RequestFn onRequest;
  DisconnectFn onDisconnect;
  // Set by the output side: a MarketDataRequest is a subscription, and
  // the output half is what acts on it (§8.10's topic question,
  // resolved FIX's own way).
  SessionSource::SubscribeFn onSubscribe;
  SessionSource::SessionReadyFn onSessionReady;

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
      // One exception to "the transport never interprets an application
      // message": MarketDataRequest (35=V) is a subscription, which is
      // how §8.10's open topic question is resolved FIX's own way
      // (§8.12 "Shape") -- a session that has requested a symbol
      // receives that topic's broadcasts, one that has not receives
      // nothing. It is still passed to the codec afterwards, since an
      // application may want to see the request too.
      if (msgTypeIs(message, "V") && this->onSubscribe) {
        const auto action = subscriptionActionOf(message);
        for (const std::string& symbol : symbolsOf(message)) {
          this->onSubscribe(connection->sessionId, symbol, action);
        }
      }

      if (!this->onRequest) {
        return;
      }
      // Otherwise the transport hands the message to the codec exactly
      // as it arrived, uninterpreted (§8.10).
      this->onRequest(std::make_shared<FixRequestContext>(
          std::string(message.message_begin(), message.message_size()), connection->sessionId,
          this->inlineResponse));
    });
    connection->session->setEventFn(
        [this, connection](SessionEvent event, DisconnectReason reason) {
          if (event == SessionEvent::LogonComplete) {
            connection->loggedOn.store(true, std::memory_order_relaxed);
            if (this->onSessionReady) {
              this->onSessionReady(connection->sessionId, *connection->session);
            }
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
      // Nagle off, on every accepted connection.
      //
      // FIX is small messages with a request/response rhythm, which is
      // exactly the shape Nagle plus the peer's delayed ACK deadlocks:
      // a small write waits for an ACK the peer holds back for up to
      // 40ms. Measured on the fleet, every session showed EXACTLY ONE
      // sample above 10ms per 40,000 -- at 40,384 / 40,608 / 41,344 /
      // 40,768 / 41,792us across five clients, which is the delayed-ACK
      // timer and nothing else. That single stall is what pushed the
      // rig's in-flight count toward its cap and produced its drop
      // count. QuickFIX sets this on its own accepted sockets
      // (SocketServer.cpp); this side never did.
      const int noDelay = 1;
      ::setsockopt(connection->socket.native_handle(), IPPROTO_TCP, TCP_NODELAY, &noDelay,
                   sizeof(noDelay));

      ::setsockopt(connection->socket.native_handle(), SOL_SOCKET, SO_RCVTIMEO, &timeout,
                   sizeof(timeout));

      std::vector<char> buffer(16 * 1024);

      // Stage timers, the counterpart of the ring reader's. That one
      // showed itself starved at 83-89% idle, which puts the limit
      // upstream -- here. This splits a session's reader thread into
      // waiting for bytes and whatever the chassis does with each
      // message (codec, signature, proposeAsync).
      using Ns = std::chrono::nanoseconds;
      const auto tick = [] { return std::chrono::steady_clock::now(); };
      std::uint64_t readNs = 0, processNs = 0, reads = 0, bytesRead = 0;
      auto lastReport = tick();
      constexpr std::uint64_t kReportIntervalNs = 2'000'000'000ULL;
      // Off unless asked for: these are diagnostics, and a sweep
      // does not want them in its logs. Set FIX_STAGE_TIMERS=1.
      const bool stageTimers = std::getenv("FIX_STAGE_TIMERS") != nullptr;

      while (!connection->stop.load(std::memory_order_relaxed) &&
             !this->stopping.load(std::memory_order_relaxed)) {
        boost::system::error_code ec;
        const auto readStart = tick();
        const std::size_t n =
            connection->socket.read_some(net::buffer(buffer.data(), buffer.size()), ec);
        readNs += static_cast<std::uint64_t>(
            std::chrono::duration_cast<Ns>(tick() - readStart).count());
        ++reads;
        bytesRead += n;
        if (ec) {
          if (ec == boost::asio::error::would_block || ec == boost::asio::error::try_again) {
            connection->session->poll();
            this->flushFor(*connection);
            continue;
          }
          break;  // peer gone
        }
        const auto processStart = tick();
        connection->session->onBytes(std::string_view(buffer.data(), n));
        connection->session->poll();
        // Backstop for anything a full socket left in outBuffer.
        this->flushFor(*connection);
        processNs += static_cast<std::uint64_t>(
            std::chrono::duration_cast<Ns>(tick() - processStart).count());

        const auto sinceReport = static_cast<std::uint64_t>(
            std::chrono::duration_cast<Ns>(tick() - lastReport).count());
        if (stageTimers && sinceReport >= kReportIntervalNs && reads > 0) {
          // Drained together so the mean covers this window only.
          const std::uint64_t proposeNs = g_proposeNs.exchange(0, std::memory_order_relaxed);
          const unsigned long long proposeCount = static_cast<unsigned long long>(
              g_proposeCount.exchange(0, std::memory_order_relaxed));
          const double proposeMeanUs =
              proposeCount > 0 ? static_cast<double>(proposeNs) / proposeCount / 1000.0 : 0.0;
          // read% high means this thread is waiting for the client.
          // process% high means the per-message chassis work -- codec,
          // signature, proposeAsync -- is what costs.
          std::fprintf(stderr,
                       "[fix-in] window=%.2fs reads=%llu bytes=%llu read=%.1f%% process=%.1f%% "
                       "process_per_read=%.1fus bytes_per_read=%.0f propose_per_msg=%.0fus "
                       "proposes=%llu\n",
                       static_cast<double>(sinceReport) / 1e9, (unsigned long long)reads,
                       (unsigned long long)bytesRead,
                       100.0 * static_cast<double>(readNs) / static_cast<double>(sinceReport),
                       100.0 * static_cast<double>(processNs) / static_cast<double>(sinceReport),
                       static_cast<double>(processNs) / reads / 1000.0,
                       static_cast<double>(bytesRead) / reads, proposeMeanUs, proposeCount);
          readNs = processNs = reads = bytesRead = 0;
          lastReport = tick();
        }
      }

      // Persist before anything else: the counters are throttled on the
      // message path, so this is what stops a dropped session resuming
      // behind where it actually got to.
      connection->session->flushSequences();

      // A socket drop with no FIX Logout: still a session loss.
      if (connection->loggedOn.exchange(false, std::memory_order_relaxed) &&
          this->onDisconnect) {
        this->onDisconnect(sequencer::SessionInfo{connection->sessionId});
      }
      std::lock_guard<std::mutex> lock(this->connectionsMutex);
      this->connections.erase(connection->sessionId);
      // Release the FIX identity too, or the client can never
      // reconnect: the guard would refuse its next Logon as a duplicate
      // of a session that no longer exists. Missing this made a
      // perfectly ordinary reconnect look like a resend failure.
      if (!connection->identityKey.empty()) {
        this->liveIdentities.erase(connection->identityKey);
      }
    });
    {
      std::lock_guard<std::mutex> lock(readersMutex);
      readers.push_back(std::move(connection->reader));
    }
  }

  // Every outbound byte goes through outBuffer, including outside a
  // batch. Writing a frame straight to the socket while buffered bytes
  // were still pending would put it on the wire AHEAD of them, which on
  // a FIX session means MsgSeqNums arriving out of order.
  void write(FixConnection& connection, std::string_view frame) {
    std::lock_guard<std::mutex> lock(connection.writeMutex);
    connection.outBuffer.append(frame.data(), frame.size());
    if (connection.batchDepth > 0) {
      return;  // endBatch() drains the lot in one go
    }
    drainLocked(connection);
  }

  // Caller holds connection.writeMutex.
  //
  // NON-BLOCKING, and that is the whole point. This used to be a
  // blocking net::write, which stalls the calling thread whenever the
  // client's socket send buffer is full. Two callers cannot afford
  // that:
  //
  //   - the propose-completion thread, when answering inline. A
  //     blocking write there turns one slow FIX client into
  //     back-pressure on the consensus pipeline itself; measured, it
  //     cost ~8x throughput (100k/s collapsed to ~12k/s).
  //   - the output side's single ring reader, which serves EVERY
  //     session. One slow client blocking it stalls delivery to all of
  //     them.
  //
  // MSG_DONTWAIT rather than putting the socket in non-blocking mode:
  // the reader thread wants its blocking read with SO_RCVTIMEO, and
  // O_NONBLOCK is a property of the file description, not of one
  // direction.
  void drainLocked(FixConnection& connection) {
    while (!connection.outBuffer.empty()) {
      const ssize_t written =
          ::send(connection.socket.native_handle(), connection.outBuffer.data(),
                 connection.outBuffer.size(), MSG_DONTWAIT | MSG_NOSIGNAL);
      if (written > 0) {
        connection.outBuffer.erase(0, static_cast<std::size_t>(written));
        continue;
      }
      if (written < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
        // Socket full. Whoever writes next drains the remainder, and
        // the reader thread's own loop is the backstop if nobody does.
        if (connection.outBuffer.size() > kMaxPendingBytes) {
          // A consumer this far behind is not going to catch up, and an
          // unbounded buffer would take the gateway down with it. Same
          // choice the broadcast ring makes for a lapped reader.
          std::fprintf(stderr,
                       "[fix] session %llu is %zu bytes behind; dropping it as a slow "
                       "consumer\n",
                       (unsigned long long)connection.sessionId, connection.outBuffer.size());
          connection.outBuffer.clear();
          connection.stop.store(true, std::memory_order_relaxed);
        }
        return;
      }
      // Peer gone; the reader thread sees the same and runs the
      // disconnect path exactly once.
      connection.outBuffer.clear();
      return;
    }
  }

  void beginBatchFor(FixConnection& connection) {
    std::lock_guard<std::mutex> lock(connection.writeMutex);
    ++connection.batchDepth;
  }

  void endBatchFor(FixConnection& connection) {
    std::lock_guard<std::mutex> lock(connection.writeMutex);
    if (connection.batchDepth > 0 && --connection.batchDepth > 0) {
      return;  // nested; the outermost flush wins
    }
    drainLocked(connection);
  }

  // Pushes out anything a full socket left behind. Called from the
  // reader thread, which wakes on inbound data or every SO_RCVTIMEO.
  void flushFor(FixConnection& connection) {
    std::lock_guard<std::mutex> lock(connection.writeMutex);
    if (connection.outBuffer.empty() || connection.batchDepth > 0) {
      return;
    }
    drainLocked(connection);
  }

  std::shared_ptr<FixConnection> connectionFor(std::uint64_t sessionId) {
    std::lock_guard<std::mutex> lock(connectionsMutex);
    const auto it = connections.find(sessionId);
    return it == connections.end() ? nullptr : it->second;
  }
};


FixInputTransport::FixInputTransport(FixInputConfig config)
    : impl_(std::make_unique<Impl>(std::move(config))) {}

FixInputTransport::~FixInputTransport() { stop(); }

void FixInputTransport::attach(RequestFn onRequest, DisconnectFn onDisconnect) {
  impl_->onRequest = std::move(onRequest);
  impl_->onDisconnect = std::move(onDisconnect);
}

void FixInputTransport::setInlineResponseFn(InlineResponseFn fn) {
  impl_->inlineResponse = std::move(fn);
}

void FixInputTransport::setAuthenticator(Authenticator authenticator) {
  impl_->authenticator = std::move(authenticator);
}

FixSession* FixInputTransport::sessionFor(std::uint64_t sessionId) {
  std::lock_guard<std::mutex> lock(impl_->connectionsMutex);
  const auto it = impl_->connections.find(sessionId);
  return it == impl_->connections.end() ? nullptr : it->second->session.get();
}

std::vector<std::uint64_t> FixInputTransport::liveSessions() {
  std::lock_guard<std::mutex> lock(impl_->connectionsMutex);
  std::vector<std::uint64_t> ids;
  ids.reserve(impl_->connections.size());
  for (const auto& [id, connection] : impl_->connections) {
    ids.push_back(id);
  }
  return ids;
}

void FixInputTransport::setSubscribeFn(SessionSource::SubscribeFn fn) {
  impl_->onSubscribe = std::move(fn);
}

void FixInputTransport::setSessionReadyFn(SessionSource::SessionReadyFn fn) {
  impl_->onSessionReady = std::move(fn);
}

void FixInputTransport::beginBatch(std::uint64_t sessionId) {
  if (auto connection = impl_->connectionFor(sessionId)) {
    impl_->beginBatchFor(*connection);
  }
}

void FixInputTransport::endBatch(std::uint64_t sessionId) {
  if (auto connection = impl_->connectionFor(sessionId)) {
    impl_->endBatchFor(*connection);
  }
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
