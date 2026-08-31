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

// The private tag carrying the harness's correlation id.
inline constexpr int kCorrelationTag = 5000;

class FixRequester : public LoadGeneratorRequester {
 public:
  FixRequester(const std::string& host, int port, std::string senderCompId,
                std::string targetCompId)
      : socket_(ioContext_) {
    boost::asio::ip::tcp::resolver resolver(ioContext_);
    boost::asio::connect(socket_, resolver.resolve(host, std::to_string(port)));

    sequencer::fix::SessionConfig config;
    config.role = sequencer::fix::Role::Initiator;
    config.senderCompId = std::move(senderCompId);
    config.targetCompId = std::move(targetCompId);
    // Heartbeats off: a benchmark run is short and continuously busy,
    // so the only thing an interval timer could do here is add work to
    // the path being measured.
    config.heartBtInt = 0;

    session_ = std::make_unique<sequencer::fix::FixSession>(
        config, store_,
        [] {
          return static_cast<std::uint64_t>(
              std::chrono::duration_cast<std::chrono::microseconds>(
                  std::chrono::steady_clock::now().time_since_epoch())
                  .count());
        });
    session_->setSendFn([this](std::string_view frame) {
      std::lock_guard<std::mutex> lock(writeMutex_);
      boost::system::error_code ec;
      boost::asio::write(socket_, boost::asio::buffer(frame.data(), frame.size()), ec);
    });
    session_->setAppMessageFn([this](const hffix::message_reader& message) { onReply(message); });
  }

  ~FixRequester() override { stop(); }

  // Logs on and starts the receive loop. Returns false if the session
  // does not establish, which the harness must treat as a configuration
  // error rather than a slow run.
  bool start() {
    reader_ = std::thread([this] { receiveLoop(); });
    session_->start();
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (!session_->isLoggedOn() && std::chrono::steady_clock::now() < deadline) {
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    return session_->isLoggedOn();
  }

  void stop() {
    if (stopping_.exchange(true)) {
      return;
    }
    boost::system::error_code ec;
    socket_.close(ec);
    if (reader_.joinable()) {
      reader_.join();
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
      std::lock_guard<std::mutex> lock(pendingMutex_);
      pending_[sequence] = std::move(onDone);
    }
    // One message, built straight into the session's own buffer. The
    // body is the correlation tag and a delta of 1 -- the smallest
    // thing the counter state machine accepts, so what is measured is
    // the transport rather than the application.
    const std::string body = std::to_string(kCorrelationTag) + "=" + std::to_string(sequence) +
                              "\001" + "5001=1\001";
    std::lock_guard<std::mutex> lock(sendMutex_);
    session_->sendApplication("U1", body);
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
  std::mutex writeMutex_;
  std::mutex sendMutex_;
  std::mutex pendingMutex_;
  // std::map, not unordered_map: the FIFO fallback above needs
  // begin() to be the OLDEST outstanding request, and the harness's
  // sequence numbers are monotonic, so ordered-by-key is ordered by
  // age.
  std::map<std::int64_t, std::function<void(bool)>> pending_;
};

}  // namespace sequencer::bench
