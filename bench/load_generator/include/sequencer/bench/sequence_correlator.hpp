#pragma once

// The transport-agnostic half of bench/load_generator/'s output-gateway
// observers (grpc_output_observer.hpp, brpc_output_observer.hpp,
// websocket_output_observer.hpp) — factored out of RelayObserver
// (relay_observer.hpp), which this deliberately does not touch: it
// already works, is independently verified against a live fleet, and
// nothing about adding output-gateway observers needs to risk it.
//
// Correlating a delivered record's payload against this same run's own
// send times, by application-assigned sequence number, is identical
// work regardless of which transport (brpc stream, real gRPC stream,
// WebSocket) delivered the payload — only *how a record arrives* (and
// how a sequence number is extracted from it) differs per transport.
// This class owns: the send-time ring, the correlator thread pool, the
// hdr histogram, the measurement window, and the two-tier reader/
// correlator split RelayObserver's own file comment explains at length
// (in short: a transport's reader path must do nothing but capture an
// arrival timestamp and enqueue — any correlation work risked inline
// on a reader collapses throughput catastrophically past a moderate
// rate, reproduced live against a real fleet before that split
// existed). A transport observer calls deliver() from wherever its own
// reader lives (a dedicated thread for gRPC/WebSocket's blocking reads,
// or directly from a brpc completion callback, which needs no reader
// thread of its own at all) — deliver() itself does only the same
// minimal capture-and-enqueue work, moving the actual extraction+wait
// to the correlator pool.
//
// Sequence-number extraction is caller-supplied (SequenceExtractor),
// not baked in here: unlike RelayObserver's raw journal bytes (a fixed,
// app-agnostic binary format), an output gateway's payload is whatever
// its OutputCodec produced — JSON for counter, arbitrary bytes for any
// other application. This mirrors LoadGeneratorRequester's own existing
// pattern of app-supplied hooks (bench/load_generator/load_generator.hpp).

#include <hdr/hdr_histogram.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

namespace sequencer::bench {

class SequenceCorrelator {
 public:
  using SequenceExtractor = std::function<std::optional<std::uint64_t>(const std::string& payload)>;

  // `extractor`: pulls the application-assigned sequence number out of
  // a delivered payload; a payload it can't parse is silently skipped
  // (not counted as a dropped race — a parse failure is a different
  // failure mode than "the record arrived but never got a matching
  // recordSend()"). `ringCapacityPow2`/`correlatorThreads`: see
  // RelayObserver's own constructor comment — identical sizing rules.
  SequenceCorrelator(SequenceExtractor extractor, std::size_t ringCapacityPow2, int correlatorThreads = 4)
      : extractor_(std::move(extractor)), correlatorThreadCount_(std::max(1, correlatorThreads)) {
    std::size_t capacity = 1;
    while (capacity < ringCapacityPow2) {
      capacity <<= 1;
    }
    mask_ = capacity - 1;
    sendTimesUs_ = std::make_unique<std::atomic<std::int64_t>[]>(capacity);
    tags_ = std::make_unique<std::atomic<std::uint64_t>[]>(capacity);
    for (std::size_t i = 0; i < capacity; ++i) {
      tags_[i].store(kUnpublished, std::memory_order_relaxed);
    }

    static constexpr std::int64_t kHighestTrackableUs = 60L * 1000L * 1000L;
    hdr_init(1, kHighestTrackableUs, 3, &observed_);
  }

  ~SequenceCorrelator() {
    stop();
    if (observed_ != nullptr) {
      hdr_close(observed_);
    }
  }

  SequenceCorrelator(const SequenceCorrelator&) = delete;
  SequenceCorrelator& operator=(const SequenceCorrelator&) = delete;

  // Starts the correlator pool only — a transport observer starts its
  // own reader (thread or callback registration) separately, after
  // this, matching RelayObserver::start()'s own ordering requirement:
  // call before the load generator starts sending.
  void start() {
    correlatorThreads_.reserve(static_cast<std::size_t>(correlatorThreadCount_));
    for (int i = 0; i < correlatorThreadCount_; ++i) {
      correlatorThreads_.emplace_back([this] { correlateLoop(); });
    }
  }

  // Same as RelayObserver::setMeasurementWindow — see there.
  void setMeasurementWindow(std::int64_t measureStartUs, std::int64_t measureEndUs) {
    measureStartUs_.store(measureStartUs, std::memory_order_relaxed);
    measureEndUs_.store(measureEndUs, std::memory_order_relaxed);
    windowSet_.store(true, std::memory_order_release);
  }

  // Marks the reader side done (no more deliver() calls coming), lets
  // the correlator pool fully drain whatever is already queued, then
  // joins it. Call only after the transport observer's own reader has
  // already stopped calling deliver() — this class has no way to know
  // that on its own, since it doesn't own the transport connection.
  // Idempotent, safe to omit (the destructor calls it too).
  void stop() {
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      readerDone_ = true;
    }
    queueCv_.notify_all();
    for (auto& t : correlatorThreads_) {
      if (t.joinable()) {
        t.join();
      }
    }
  }

  // Same as RelayObserver::recordSend — see there. Wait-free, safe
  // from a latency-sensitive completion callback.
  void recordSend(std::uint64_t sequenceNumber, std::int64_t sendTimeUs) noexcept {
    const std::size_t slot = sequenceNumber & mask_;
    sendTimesUs_[slot].store(sendTimeUs, std::memory_order_relaxed);
    tags_[slot].store(sequenceNumber, std::memory_order_release);
    std::uint64_t floor = firstSeenSeq_.load(std::memory_order_relaxed);
    while ((floor == 0 || sequenceNumber < floor) &&
           !firstSeenSeq_.compare_exchange_weak(floor, sequenceNumber, std::memory_order_relaxed)) {
    }
  }

  // Called by a transport observer's own reader (thread or callback)
  // once per delivered record, immediately after capturing arrivalUs —
  // this must be the only work done before enqueueing; see this file's
  // own top comment for why correlation itself must never run inline
  // here. Thread-safe: brpc's own callback thread and a dedicated
  // reader thread are both valid callers.
  void deliver(const std::string& payload, std::int64_t arrivalUs) {
    std::size_t queueSize;
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      queue_.push_back(PendingCorrelation{payload, arrivalUs});
      queueSize = queue_.size();
    }
    queueCv_.notify_one();
    std::int64_t prevHigh = queueHighWater_.load(std::memory_order_relaxed);
    while (static_cast<std::int64_t>(queueSize) > prevHigh &&
           !queueHighWater_.compare_exchange_weak(prevHigh, static_cast<std::int64_t>(queueSize),
                                                    std::memory_order_relaxed)) {
    }
  }

  // Prints the round-trip percentile summary, fields namespaced by
  // `labelPrefix` (e.g. "output_grpc" -> "output_grpc_p50_us") so a
  // sweep script's whitespace-anchored grep can never confuse one
  // transport's numbers for another's — same discipline RelayObserver's
  // own "relay_p50_us" naming exists for (see
  // bench/load_generator/README.md). Call after both the load
  // generator's own run() and this object's stop() have returned.
  void printSummary(const char* labelPrefix) const {
    std::printf("%s_completed=%ld\n", labelPrefix, static_cast<long>(observed_->total_count));
    std::printf("%s_dropped_races=%ld\n", labelPrefix, static_cast<long>(droppedRaces_.load(std::memory_order_relaxed)));
    std::printf("%s_skipped_historical=%ld\n", labelPrefix,
                 static_cast<long>(skippedHistorical_.load(std::memory_order_relaxed)));
    std::printf("%s_unparseable=%ld\n", labelPrefix, static_cast<long>(unparseable_.load(std::memory_order_relaxed)));
    std::printf("%s_queue_high_water=%ld\n", labelPrefix,
                 static_cast<long>(queueHighWater_.load(std::memory_order_relaxed)));
    std::printf("%s_p50_us=%ld\n", labelPrefix, static_cast<long>(hdr_value_at_percentile(observed_, 50.0)));
    std::printf("%s_p90_us=%ld\n", labelPrefix, static_cast<long>(hdr_value_at_percentile(observed_, 90.0)));
    std::printf("%s_p99_us=%ld\n", labelPrefix, static_cast<long>(hdr_value_at_percentile(observed_, 99.0)));
    std::printf("%s_p99_9_us=%ld\n", labelPrefix, static_cast<long>(hdr_value_at_percentile(observed_, 99.9)));
    std::printf("%s_max_us=%ld\n", labelPrefix, static_cast<long>(hdr_max(observed_)));
    std::printf("%s_mean_us=%ld\n", labelPrefix, static_cast<long>(hdr_mean(observed_)));
  }

  // The OBSERVED histogram as bucket midpoints and counts, in the same
  // "value,count" form LoadGenerator::hdrRawOut writes, so sweep/
  // merge-hdr.py can merge several clients' files into one.
  //
  // This exists because a multi-client sweep cannot use the printed
  // percentiles above: percentiles do not average. Merging the
  // histograms is the only way to get a true p99 across five clients,
  // and until now only the ACK path could be merged -- the output arm's
  // latency lived solely in those _p50_us lines, which is why the
  // output-gateway sweeps ran single-client.
  bool writeRawHistogram(const std::string& path) const {
    if (path.empty()) {
      return true;
    }
    FILE* f = std::fopen(path.c_str(), "w");
    if (f == nullptr) {
      return false;
    }
    std::fprintf(f, "value,count\n");
    hdr_iter iter;
    hdr_iter_recorded_init(&iter, observed_);
    while (hdr_iter_next(&iter)) {
      if (iter.count > 0) {
        std::fprintf(f, "%lld,%lld\n", static_cast<long long>(iter.value),
                     static_cast<long long>(iter.count));
      }
    }
    std::fclose(f);
    return true;
  }

 private:
  static constexpr std::uint64_t kUnpublished = static_cast<std::uint64_t>(-1);

  struct PendingCorrelation {
    std::string payload;
    std::int64_t arrivalUs;
  };

  // One of correlatorThreadCount_ identical workers — see
  // RelayObserver::correlateLoop()'s own comment for the historical-
  // skip / bounded-wait / histogram-record shape this mirrors exactly,
  // with one addition: extraction happens here too (deliver() only
  // captures the raw payload), so an unparseable payload is a third
  // possible outcome alongside "correlated" and "dropped race".
  void correlateLoop() {
    while (true) {
      PendingCorrelation item;
      {
        std::unique_lock<std::mutex> lock(queueMutex_);
        queueCv_.wait(lock, [this] { return !queue_.empty() || readerDone_; });
        if (queue_.empty()) {
          return;
        }
        item = std::move(queue_.front());
        queue_.pop_front();
      }

      const std::optional<std::uint64_t> seq = extractor_(item.payload);
      if (!seq.has_value()) {
        unparseable_.fetch_add(1, std::memory_order_relaxed);
        continue;
      }

      const std::size_t slot = *seq & mask_;
      const std::uint64_t floor = firstSeenSeq_.load(std::memory_order_relaxed);
      if (floor != 0 && *seq < floor) {
        skippedHistorical_.fetch_add(1, std::memory_order_relaxed);
        continue;
      }

      if (waitForTag(slot, *seq)) {
        const std::int64_t sendTimeUs = sendTimesUs_[slot].load(std::memory_order_relaxed);
        if (inMeasurementWindow(sendTimeUs)) {
          const std::int64_t latencyUs = std::max<std::int64_t>(1, item.arrivalUs - sendTimeUs);
          hdr_record_value_atomic(observed_, latencyUs);
        }
      } else {
        droppedRaces_.fetch_add(1, std::memory_order_relaxed);
      }
    }
  }

  // Identical to RelayObserver::waitForTag — see there for the full
  // rationale (spin first, then bounded backoff; a genuine race
  // against a slow-tail ack is worth waiting for, not giving up on
  // early).
  bool waitForTag(std::size_t slot, std::uint64_t seq) const {
    constexpr int kSpinIterations = 20000;
    constexpr auto kMaxWait = std::chrono::milliseconds(100);
    int spins = 0;
    std::chrono::steady_clock::time_point backoffStart{};
    while (tags_[slot].load(std::memory_order_acquire) != seq) {
      if (spins < kSpinIterations) {
        ++spins;
        continue;
      }
      const auto now = std::chrono::steady_clock::now();
      if (spins == kSpinIterations) {
        backoffStart = now;
        ++spins;
      }
      if (now - backoffStart >= kMaxWait) {
        return false;
      }
      std::this_thread::sleep_for(std::chrono::microseconds(50));
    }
    return true;
  }

  bool inMeasurementWindow(std::int64_t sendTimeUs) const {
    if (!windowSet_.load(std::memory_order_acquire)) {
      return true;
    }
    return sendTimeUs >= measureStartUs_.load(std::memory_order_relaxed) &&
           sendTimeUs <= measureEndUs_.load(std::memory_order_relaxed);
  }

  SequenceExtractor extractor_;
  int correlatorThreadCount_;
  std::size_t mask_ = 0;
  std::unique_ptr<std::atomic<std::int64_t>[]> sendTimesUs_;
  std::unique_ptr<std::atomic<std::uint64_t>[]> tags_;

  struct hdr_histogram* observed_ = nullptr;
  std::atomic<std::int64_t> droppedRaces_{0};
  std::atomic<std::int64_t> skippedHistorical_{0};
  std::atomic<std::int64_t> unparseable_{0};
  std::atomic<std::int64_t> queueHighWater_{0};
  std::atomic<std::uint64_t> firstSeenSeq_{0};
  std::atomic<bool> windowSet_{false};
  std::atomic<std::int64_t> measureStartUs_{0};
  std::atomic<std::int64_t> measureEndUs_{0};

  std::deque<PendingCorrelation> queue_;
  std::mutex queueMutex_;
  std::condition_variable queueCv_;
  bool readerDone_ = false;  // guarded by queueMutex_

  std::vector<std::thread> correlatorThreads_;
};

// A tiny shared interface, justified only by load_generator_main.cpp
// needing to hold and call exactly one of three otherwise-unrelated
// concrete transport observers uniformly (recordSend() from a
// completion callback, start()/stop() around the load generator's own
// run(), printSummary() after) — not a speculative abstraction; see
// grpc_output_observer.hpp / brpc_output_observer.hpp /
// websocket_output_observer.hpp for the concrete implementations, each
// composing one SequenceCorrelator rather than inheriting from it.
class OutputGatewayObserver {
 public:
  virtual ~OutputGatewayObserver() = default;
  virtual void start() = 0;
  virtual void stop() = 0;
  virtual void setMeasurementWindow(std::int64_t measureStartUs, std::int64_t measureEndUs) = 0;
  virtual void recordSend(std::uint64_t sequenceNumber, std::int64_t sendTimeUs) noexcept = 0;
  virtual void printSummary() const = 0;

  // Writes the observer's own histogram in merge-hdr.py's format, so a
  // multi-client sweep can merge the OUTPUT path's latency rather than
  // averaging per-client percentiles (which is not a thing you can do).
  virtual bool writeRawHistogram(const std::string& path) const = 0;
};

}  // namespace sequencer::bench
