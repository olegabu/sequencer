#pragma once

// A FIX load sender: LoadGeneratorRequester over the SAME session core
// the gateway's acceptor runs, in Initiator role.
//
// This is specification.md §8.12's reason 2 made concrete. A QuickFIX
// initiator cannot serve as this rig -- its allocation-heavy,
// one-thread-per-session design tops out around tens of thousands of
// messages per second and adds jitter of its own, so the rig would be
// the bottleneck before the gateway is, and every number it produced
// about the gateway would be a number about QuickFIX. Building the
// session layer once, usable in both roles, is what makes the gateway's
// half nearly free AND gives the harness a sender fast enough to
// measure with.
//
// Correlation is by a user-defined tag (5000-9999, per FIX convention
// for private tags): the sender stamps tag 5000 with the harness's own
// sequence, and the reply carries it back, so a completion is matched
// without depending on message ORDER -- which a session gateway is free
// to vary, since outputs arrive in journal order rather than request
// order.

#include <sequencer/bench/load_generator.hpp>
#include <sequencer/fix/fix_session.hpp>

#include <netinet/in.h>
#include <netinet/tcp.h>

#include <boost/asio/connect.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/write.hpp>

#include <sys/socket.h>
#include <sys/time.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace sequencer::bench {

// The tag a reply carries its correlation value in. The application's
// output codec must echo it; examples/counter does
// (kCounterEchoTag in counter_fix_codecs.hpp).
inline constexpr int kCorrelationTag = 5000;

// The tag this sender puts its payload in. For examples/counter that is
// the delta, and the delta doubles as the correlation value -- see
// send().
inline constexpr int kCounterPayloadTag = 5001;

class FixRequester : public LoadGeneratorRequester {
 public:
  // `clientId` identifies THIS session among all of them. It goes in
  // the HIGH BITS of the value sent as the application payload:
  //
  //     payload = (clientId << clientIdShift) | sequence
  //
  // which does two jobs at once. The reply echoes the whole value back,
  // so the low bits correlate a reply to its request; and the
  // application's output codec recovers the high bits to publish on
  // this session's OWN topic, so a session receives only its own
  // replies. Without that second part every session receives every
  // reply and a gateway's delivery load becomes (rate x sessions).
  //
  // Must be unique per session in a multi-session run. Sharing it
  // silently makes sessions complete each other's requests, which reads
  // as impossibly good latency rather than as an error.
  FixRequester(const std::string& host, int port, std::string senderCompId,
                std::string targetCompId, std::int64_t clientId = 0,
                int clientIdShift = 40)
      : socket_(ioContext_), clientId_(clientId), clientIdShift_(clientIdShift) {
    boost::asio::ip::tcp::resolver resolver(ioContext_);
    boost::asio::connect(socket_, resolver.resolve(host, std::to_string(port)));
    // Nagle off. The client is the side that both FIX arms share, so a
    // delayed-ACK stall here shows up in every FIX measurement this
    // repository makes -- and it did: one sample per session per run at
    // ~40ms, in the hffix and QuickFIX sweeps alike. See
    // fix_input_transport.cpp for the numbers.
    {
      const int noDelay = 1;
      ::setsockopt(socket_.native_handle(), IPPROTO_TCP, TCP_NODELAY, &noDelay, sizeof(noDelay));
    }

    sequencer::fix::SessionConfig config;
    config.role = sequencer::fix::Role::Initiator;
    config.senderCompId = std::move(senderCompId);
    config.targetCompId = std::move(targetCompId);
    // Heartbeats off: a benchmark run is short and continuously busy,
    // so the only thing an interval timer could do here is add work to
    // the path being measured.
    config.heartBtInt = 0;

    // ResetSeqNumFlag on Logon, and this is NOT optional for a rig.
    //
    // Each run is a fresh process with fresh in-memory counters, while
    // the gateway persists this CompID's numbers across runs. Without
    // the reset the client logs on at MsgSeqNum 1 against a gateway
    // expecting thousands, which is MsgSeqNum-too-low -- unrecoverable
    // in FIX, so the gateway logs it out and drops it. 141=Y is exactly
    // the mechanism for "I have no history, start us both at 1".
    //
    // Its absence cost a great deal here. Every benchmark run after the
    // FIRST against a given gateway was measuring a session the gateway
    // had already refused: sends went nowhere, replies never came, and
    // the harness reported it as dropped-by-rig. That was read as the
    // gateway collapsing above 1,000-2,000 msg/s, and several rounds of
    // profiling chased it. Distinct CompIDs per run hid it; repeating
    // one run exposed it in three lines.
    config.resetSeqNumOnLogon = true;

    session_ = std::make_unique<sequencer::fix::FixSession>(
        config, store_,
        [] {
          return static_cast<std::uint64_t>(
              std::chrono::duration_cast<std::chrono::microseconds>(
                  std::chrono::steady_clock::now().time_since_epoch())
                  .count());
        });
    // Encoded frames go into a buffer, NOT onto the socket. The write
    // syscall happens on the writer thread below, so it is off the
    // critical section every sender is serialized through.
    session_->setSendFn([this](std::string_view frame) {
      std::lock_guard<std::mutex> lock(outMutex_);
      outBuffer_.append(frame.data(), frame.size());
    });
    session_->setAppMessageFn([this](const hffix::message_reader& message) { onReply(message); });
  }

  ~FixRequester() override { stop(); }

  // Logs on and starts the receive loop. Returns false if the session
  // does not establish, which the harness must treat as a configuration
  // error rather than a slow run.
  bool start() {
    reader_ = std::thread([this] { receiveLoop(); });
    writer_ = std::thread([this] { writeLoop(); });
    session_->start();
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (!session_->isLoggedOn() && std::chrono::steady_clock::now() < deadline) {
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    if (!session_->isLoggedOn()) {
      return false;
    }
    // Confirm the session SURVIVES, rather than trusting the moment it
    // came up. A gateway that rejects the logon on sequence grounds
    // replies with a valid Logon echo first and disconnects immediately
    // after, so isLoggedOn() is briefly true for a session that is
    // already dead -- which is how a whole benchmark run could be spent
    // sending into a closed session and reporting it as the gateway's
    // fault.
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    return session_->isLoggedOn();
  }

  void stop() {
    if (stopping_.exchange(true)) {
      return;
    }
    // shutdown() BEFORE close(), and the order matters. close() alone
    // does not reliably wake a thread parked in a read on Linux -- it
    // drops this process's handle without disturbing the blocked call
    // -- whereas shutdown() tears down the connection itself and makes
    // the pending read return at once. Closing alone left the load
    // generator printing its summary and then hanging forever on the
    // join below, which a sweep script would wait on indefinitely.
    boost::system::error_code ec;
    ::shutdown(socket_.native_handle(), SHUT_RDWR);
    socket_.close(ec);
    if (reader_.joinable()) {
      reader_.join();
    }
    if (writer_.joinable()) {
      writer_.join();
    }
  }

  // Subscribes to a broadcast topic by its FIX-standard mechanism: a
  // MarketDataRequest naming the topic as a Symbol (tag 55). Needed
  // whenever the application under test broadcasts its replies rather
  // than addressing the submitting session -- examples/counter does,
  // for the reason its codec header gives.
  void subscribe(const std::string& symbol) {
    std::lock_guard<std::mutex> lock(sendMutex_);
    session_->sendApplication("V", "262=loadgen\001263=1\001146=1\00155=" + symbol + "\001");
  }

  void send(std::int64_t sequence, std::int64_t /*sendTimeUs*/,
             std::function<void(bool ok)> onDone) override {
    {
      // Keyed by the nonce, which is what comes back, rather than by
      // the harness sequence.
      std::lock_guard<std::mutex> lock(pendingMutex_);
      pending_[(clientId_ << clientIdShift_) | sequence] = std::move(onDone);
    }
    // One message. The body is the correlation tag and a delta of 1 --
    // the smallest thing the counter state machine accepts, so what is
    // measured is the transport rather than the application.
    //
    // Built OUTSIDE the lock: a FIX session must assign MsgSeqNum and
    // emit bytes in one order, so sendApplication() has to be
    // serialized, but nothing else does. Formatting the body under the
    // lock made every sender wait on every other sender's string work.
    // The payload IS the correlation value, not a constant.
    //
    // examples/counter's input is eight bytes of delta and nothing
    // else, and its state machine rejects any other size, so there is
    // nowhere to put a separate nonce without changing the state
    // machine and the journaled encoding. Its output codec therefore
    // echoes the submitted delta back (kCounterEchoTag), and this
    // sends a distinct delta per request so the echo identifies it.
    //
    // Sending a constant 1 -- which this did -- makes every reply
    // indistinguishable, so correlation falls through to matching by
    // arrival order. With several clients on one broadcast topic that
    // is simply wrong, and it produced a measured p50 of 75us on a
    // fleet whose leader-to-follower RTT alone is 500us. An impossible
    // number was the only thing that gave it away.
    const std::int64_t nonce = (clientId_ << clientIdShift_) | sequence;
    const std::string body = std::to_string(kCounterPayloadTag) + "=" +
                              std::to_string(nonce) + "\001";
    {
      // The critical section is now encode-and-append only: no syscall,
      // no allocation of the body, no formatting. The writer thread
      // owns the socket.
      std::lock_guard<std::mutex> lock(sendMutex_);
      session_->sendApplication("U1", body);
    }
  }

 private:
  void onReply(const hffix::message_reader& message) {
    std::int64_t correlation = -1;
    for (auto it = message.begin(); it != message.end(); ++it) {
      if (it->tag() == kCorrelationTag) {
        correlation = std::strtoll(
            std::string(it->value().begin(), it->value().size()).c_str(), nullptr, 10);
      }
    }
    std::function<void(bool)> done;
    {
      std::lock_guard<std::mutex> lock(pendingMutex_);
      if (correlation >= 0) {
        const auto it = pending_.find(correlation);
        if (it == pending_.end()) {
          return;  // already completed, or a duplicate
        }
        done = std::move(it->second);
        pending_.erase(it);
      } else {
        // FIFO fallback: the reply carries no correlation tag, so
        // complete the OLDEST outstanding request.
        //
        // This exists because an application's reply cannot always
        // carry one. examples/counter is the case in point: its input
        // is exactly eight bytes -- a delta and nothing else -- so the
        // journal record holds no correlation id and its OutputCodec
        // has none to echo (see counter_fix_codecs.hpp).
        //
        // VALID ONLY FOR ONE SESSION AT A TIME, and that condition is
        // load-bearing. Outputs arrive in journal order and a single
        // session submits in order, so the k-th reply belongs to the
        // k-th request. Across several sessions sharing a broadcast
        // topic it is simply wrong -- each would complete requests
        // against other clients' replies -- so a multi-session FIX
        // sweep needs an application whose reply echoes a correlation
        // tag, which is what kCorrelationTag is for.
        if (pending_.empty()) {
          return;
        }
        const auto oldest = pending_.begin();
        done = std::move(oldest->second);
        pending_.erase(oldest);
      }
    }
    if (done) {
      done(true);
    }
  }

  // Drains whatever has been encoded and writes it as ONE syscall.
  //
  // This is the rule the relay, output and input gateways all arrived
  // at -- gather what is available now, send once, never delay a send
  // to wait for more -- applied to the rig, which needed it as much as
  // they did. With the write inline under the send lock, adding sender
  // threads made throughput WORSE: at 2,000/s one thread carried it
  // with zero drops while two dropped 9,001 (gateway/fix/README.md).
  void writeLoop() {
    std::string batch;
    while (!stopping_.load(std::memory_order_relaxed)) {
      {
        std::lock_guard<std::mutex> lock(outMutex_);
        batch.swap(outBuffer_);
        outBuffer_.clear();
      }
      if (batch.empty()) {
        // Nothing waiting. A short sleep rather than a spin: this
        // thread is not latency-critical, it is throughput-critical,
        // and burning a core here would take one from the senders.
        std::this_thread::sleep_for(std::chrono::microseconds(50));
        continue;
      }
      boost::system::error_code ec;
      boost::asio::write(socket_, boost::asio::buffer(batch.data(), batch.size()), ec);
      batch.clear();
      if (ec) {
        return;
      }
    }
  }

  void receiveLoop() {
    // A receive timeout so the blocking read returns periodically and
    // the loop can see stopping_. Closing the socket from another
    // thread does NOT reliably wake a thread parked in read_some --
    // the same trap that deadlocked FixInputTransport::stop() earlier,
    // reproduced here because this file was written from the same
    // assumption. Without it the load generator ran fine and then hung
    // forever at shutdown, never printing its summary.
    struct timeval timeout {};
    timeout.tv_sec = 0;
    timeout.tv_usec = 200000;
    ::setsockopt(socket_.native_handle(), SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));

    std::vector<char> buffer(64 * 1024);
    while (!stopping_.load(std::memory_order_relaxed)) {
      boost::system::error_code ec;
      const std::size_t n =
          socket_.read_some(boost::asio::buffer(buffer.data(), buffer.size()), ec);
      if (ec) {
        if (ec == boost::asio::error::would_block || ec == boost::asio::error::try_again) {
          continue;
        }
        return;
      }
      session_->onBytes(std::string_view(buffer.data(), n));
    }
  }

  class MemoryStore : public sequencer::fix::SequenceStore {
   public:
    sequencer::fix::SequenceNumbers load(const std::string& key) override { return numbers_[key]; }
    void store(const std::string& key, const sequencer::fix::SequenceNumbers& n) override {
      numbers_[key] = n;
    }

   private:
    std::map<std::string, sequencer::fix::SequenceNumbers> numbers_;
  };

  boost::asio::io_context ioContext_;
  boost::asio::ip::tcp::socket socket_;
  MemoryStore store_;
  std::unique_ptr<sequencer::fix::FixSession> session_;
  std::thread reader_;
  std::atomic<bool> stopping_{false};
  std::mutex sendMutex_;   // serializes MsgSeqNum assignment and encoding
  std::mutex outMutex_;    // guards the encoded-bytes buffer
  std::string outBuffer_;
  const std::int64_t clientId_ = 0;
  const int clientIdShift_ = 40;
  std::thread writer_;
  std::mutex pendingMutex_;
  // std::map, not unordered_map: the FIFO fallback above needs
  // begin() to be the OLDEST outstanding request, and the harness's
  // sequence numbers are monotonic, so ordered-by-key is ordered by
  // age.
  std::map<std::int64_t, std::function<void(bool)>> pending_;
};

// Spreads one harness's requests round-robin across several FIX
// sessions.
//
// It exists to keep gateways EVENLY loaded. With one session per client
// and an odd number of clients, some gateway always draws more clients
// than another -- five clients over two gateways is a 3/2 split, so one
// carries 60% of the offered rate -- and the merged latency then blends
// a busy gateway with a quiet one, which is neither gateway's real
// number. Giving every client a session on every gateway makes the
// split exact.
class FixFanoutRequester : public LoadGeneratorRequester {
 public:
  void add(std::unique_ptr<FixRequester> session) { sessions_.push_back(std::move(session)); }

  void send(std::int64_t sequence, std::int64_t sendTimeUs,
             std::function<void(bool ok)> onDone) override {
    // By sequence, not by a shared counter: the harness's sequence is
    // already monotonic, so this needs no synchronization of its own
    // and distributes exactly evenly.
    const std::size_t which =
        static_cast<std::size_t>(sequence) % sessions_.size();
    sessions_[which]->send(sequence, sendTimeUs, std::move(onDone));
  }

 private:
  std::vector<std::unique_ptr<FixRequester>> sessions_;
};

}  // namespace sequencer::bench
