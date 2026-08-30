#pragma once

// One RelaySession per active Subscribe call (specification.md §8.2):
// owns a dedicated tailing thread reading from a shared, colocated
// JournalReader starting at its own fromSequenceNumber, writing each
// record's raw bytes to its own brpc::Stream, strictly in order.
// Independent per-session cursors — not a single shared broadcast, the
// way gateway/output's Fanout works — are what let a relay serve "any
// number of concurrent remote subscribers" (§8.2) each resuming from
// wherever they individually left off, including from arbitrary
// already-committed history, not just what's live.
//
// Safety note: this stream's on_closed() callback is the same brpc
// contract gap documented twice already in this repository
// (gateway/output/README.md's BrpcStreamFanout::closeAll(), and
// gateway/output/tests/collecting_stream_client.hpp's client-side
// mirror of it) — StreamClose() does not guarantee on_closed() has
// already run by the time it returns. This class's destructor closes
// the stream and blocks, bounded, until on_closed() actually confirms,
// before anything on_closed() touches can be freed.

#include <brpc/stream.h>

#include <sequencer/journal/reader.hpp>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <thread>

namespace sequencer::gateway::relay::detail {

class RelaySession : public brpc::StreamInputHandler {
 public:
  RelaySession(std::shared_ptr<journal::JournalReader> reader, std::uint64_t fromSequenceNumber)
      : reader_(std::move(reader)), nextSeq_(fromSequenceNumber == 0 ? 1 : fromSequenceNumber) {}

  RelaySession(const RelaySession&) = delete;
  RelaySession& operator=(const RelaySession&) = delete;

  ~RelaySession() override {
    stopRequested_.store(true, std::memory_order_relaxed);
    if (tailThread_.joinable()) {
      tailThread_.join();
    }
    if (streamId_ != brpc::INVALID_STREAM_ID) {
      brpc::StreamClose(streamId_);
      std::unique_lock<std::mutex> lock(mutex_);
      closedCv_.wait_for(lock, std::chrono::seconds(5), [this] { return closed_; });
    }
  }

  // Called once, right after a successful brpc::StreamAccept.
  void start(brpc::StreamId streamId) {
    streamId_ = streamId;
    tailThread_ = std::thread([this] { tailLoop(); });
  }

  bool isClosed() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return closed_;
  }

  // brpc::StreamInputHandler: a relay's subscribers never send
  // anything back over the stream — inbound messages are simply
  // ignored, not an error, since a well-behaved client only reads.
  int on_received_messages(brpc::StreamId, butil::IOBuf* const[], size_t) override { return 0; }
  void on_idle_timeout(brpc::StreamId) override {}
  void on_closed(brpc::StreamId) override {
    std::lock_guard<std::mutex> lock(mutex_);
    closed_ = true;
    closedCv_.notify_all();
    // Stop tailing promptly rather than waiting for the loop's next
    // poll — the remote side is gone regardless of what's left to send.
    stopRequested_.store(true, std::memory_order_relaxed);
  }

 private:
  void tailLoop() {
    while (!stopRequested_.load(std::memory_order_relaxed)) {
      if (!reader_->contains(nextSeq_)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        continue;
      }
      const journal::RecordView record = reader_->record(nextSeq_);
      if (!writeRecord(record.rawBytes())) {
        break;  // stream closed/invalid; on_closed() (if it fires) will confirm
      }
      ++nextSeq_;
    }
  }

  // Bounded retry on EAGAIN (specification.md §8.2's "support any
  // number of concurrent remote subscribers" implies a slow one must
  // be handled gracefully, not by dropping records) — brpc::StreamWrite
  // returns EAGAIN when the remote side hasn't drained enough of what
  // was already sent. Any other failure (EINVAL: closed/invalid
  // stream) is unrecoverable for this session.
  bool writeRecord(Payload rawBytes) {
    butil::IOBuf buf;
    buf.append(rawBytes.data(), rawBytes.size());
    for (int attempt = 0; attempt < 1000 && !stopRequested_.load(std::memory_order_relaxed); ++attempt) {
      const int rc = brpc::StreamWrite(streamId_, buf);
      if (rc == 0) {
        return true;
      }
      if (errno != EAGAIN) {
        return false;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    return false;
  }

  std::shared_ptr<journal::JournalReader> reader_;
  std::uint64_t nextSeq_;
  brpc::StreamId streamId_ = brpc::INVALID_STREAM_ID;
  std::thread tailThread_;
  std::atomic<bool> stopRequested_{false};
  mutable std::mutex mutex_;
  std::condition_variable closedCv_;
  bool closed_ = false;
};

}  // namespace sequencer::gateway::relay::detail
