#pragma once

// The third round trip bench/load_generator/README.md describes:
// submission to receipt via the relay gateway's real gRPC stream
// (specification.md §8.9), observed on the *same machine* as the
// client. Deliberately not a gateway — no codec, no client protocol
// translation, no application meaning — just a background
// grpc::ClientReader<RecordBatch> loop, the same batch-shaped read
// relay_grpc_test.cpp's own TestGrpcRelayClient already proves out,
// correlating each arrival against this same run's own send times by
// journal sequence number.
//
// A separate, opt-in library target from sequencer_bench_load_generator
// (see this directory's CMakeLists.txt): it pulls in gRPC and
// gateway/relay's generated proto code, a heavier dependency an
// application not comparing against the relay path has no reason to
// link.
//
// Two threads, deliberately, not one: reading the gRPC stream and
// correlating what it delivers are split across a reader thread and a
// small pool of correlator threads, connected by a queue — reproduced
// live, against a real fleet, doing both inline on the reader thread
// (this file's own earlier design) collapsed entirely above ~25k
// req/s: p90 exploded 150x between 25k and 40k while p50 barely
// moved (the signature of a single thread's own processing falling
// behind a stream, backlog compounding over the run — later records
// look worse than earlier ones — not genuine per-record dissemination
// lag), and above 70k it correlated *zero* records for the whole run.
// The fix follows directly from where the timestamp is captured:
// nowMicros() already runs immediately after Read() returns, before
// any waiting — so as long as that capture stays on the reader
// thread, moving the (potentially blocking) correlation work
// elsewhere costs no accuracy, only wall-clock time for this object's
// own bookkeeping to finish, which is not what gets measured.
//
// Thread synchronization, precisely: recordSend() runs on whichever
// thread learns a request's assigned journal sequence number (a brpc
// completion callback, in examples/counter's SubmitRequester); the
// reader thread owns the gRPC stream exclusively; a pool of correlator
// threads pull queued (sequence, arrival time) pairs and race against
// recordSend() on the same lock-free ring recordSend() itself is used
// by. All three race genuinely, not just in theory: the relay tails
// the journal directly and can disseminate a record *before* the
// synchronous acknowledgement has finished its own trip back through
// the input gateway to the client — exactly the fast-relay-delivery
// case this class exists to measure, so it must not be silently
// dropped as a lost race. See recordSend()/waitForTag()'s own comments
// for the ring buffer and bounded-wait design that follows from that.

#include <grpcpp/grpcpp.h>
#include <hdr/hdr_histogram.h>

#include <sequencer/journal/record_view.hpp>

#include "relay_grpc.grpc.pb.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace sequencer::bench {

class RelayObserver {
 public:
  // `relayGrpcAddr`: the relay's real-gRPC listen address ("host:port").
  // `fromSequenceNumber`: 0 subscribes from the beginning of the
  // journal (matching relay semantics exactly) — fine, if slower to
  // catch up, against a journal with little prior history; pass the
  // first sequence number this run's own requests will plausibly be
  // assigned to skip straight to it against a journal with a lot.
  // `ringCapacityPow2` is rounded up to the next power of two and
  // becomes the send-time ring's size — size it to comfortably exceed
  // the total number of requests this run will ever issue (rate x
  // duration, for open loop): recordSend() and the correlator pool tag
  // each slot with the journal sequence number currently occupying
  // it, so an undersized ring degrades to silently-dropped samples
  // (a stale tag never matches) rather than misattributed ones, but
  // sized correctly there is no wraparound at all within one run,
  // since sequence numbers are assigned once each and never reused.
  // `correlatorThreads`: how many threads pull off the queue and run
  // waitForTag() concurrently. More than one matters because a
  // genuine race's wait is wall-clock time the CPU cannot shorten —
  // several correlators can each wait on their own record at once,
  // which is exactly the throughput a single reader-thread-does-
  // everything design cannot get no matter how little each individual
  // wait costs.
  RelayObserver(std::string relayGrpcAddr, std::uint64_t fromSequenceNumber, std::size_t ringCapacityPow2,
                int correlatorThreads = 4)
      : relayGrpcAddr_(std::move(relayGrpcAddr)),
        fromSequenceNumber_(fromSequenceNumber),
        correlatorThreadCount_(std::max(1, correlatorThreads)) {
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

  ~RelayObserver() {
    stop();
    if (observed_ != nullptr) {
      hdr_close(observed_);
    }
  }

  RelayObserver(const RelayObserver&) = delete;
  RelayObserver& operator=(const RelayObserver&) = delete;

  // Connects and starts the reader thread and the correlator pool.
  // Call once, before the load generator starts sending — a request
  // whose relay delivery races ahead of this call starting would
  // otherwise be missed.
  void start() {
    auto channel = grpc::CreateChannel(relayGrpcAddr_, grpc::InsecureChannelCredentials());
    stub_ = gateway::relay::grpc_proto::RelayService::NewStub(channel);
    readerThread_ = std::thread([this] { readLoop(); });
    correlatorThreads_.reserve(static_cast<std::size_t>(correlatorThreadCount_));
    for (int i = 0; i < correlatorThreadCount_; ++i) {
      correlatorThreads_.emplace_back([this] { correlateLoop(); });
    }
  }

  // Restricts histogram recording to sends whose reference time falls
  // in [measureStartUs, measureEndUs] — the same warmup-excluded
  // window LoadGenerator's own histogram uses, so the two summaries
  // describe the same span and are actually comparable. Without this,
  // every warmup-phase request the relay happens to deliver (all of
  // them, typically, since the relay has no warmup concept of its
  // own) would inflate relay_completed and skew percentiles toward
  // cold-start latency the main summary deliberately excludes.
  // Optional: unset (the default) records everything, which is fine
  // for a caller not doing a windowed comparison. recordSend() itself
  // is unaffected either way — every sample is still tracked for
  // correlation, just not necessarily tallied into the histogram.
  void setMeasurementWindow(std::int64_t measureStartUs, std::int64_t measureEndUs) {
    measureStartUs_.store(measureStartUs, std::memory_order_relaxed);
    measureEndUs_.store(measureEndUs, std::memory_order_relaxed);
    windowSet_.store(true, std::memory_order_release);
  }

  // Cancels the streaming call, joins the reader thread, then lets the
  // correlator pool fully drain whatever is still queued (not abandon
  // it — a queued item already has its true arrival timestamp
  // captured, so finishing it costs only this call's own wall-clock
  // time, not measurement accuracy) before joining those too. Safe to
  // call more than once (idempotent) and safe to omit — the
  // destructor calls it too.
  void stop() {
    if (!stopRequested_.exchange(true, std::memory_order_relaxed)) {
      context_.TryCancel();
    }
    if (readerThread_.joinable()) {
      readerThread_.join();
    }
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

  // Called from any sending thread, once per request, as soon as the
  // journal sequence number that request was assigned becomes known —
  // NOT at send time, since (unlike the load generator's own
  // synchronous-receipt latency) the journal sequence number isn't
  // known until the response arrives; see load_generator.hpp's
  // LoadGeneratorRequester comment for why `sendTimeUs` is threaded
  // all the way through to here instead. Wait-free: two relaxed/
  // release atomic stores, no lock, no allocation — safe to call from
  // a latency-sensitive completion callback.
  void recordSend(std::uint64_t journalSequenceNumber, std::int64_t sendTimeUs) noexcept {
    const std::size_t slot = journalSequenceNumber & mask_;
    sendTimesUs_[slot].store(sendTimeUs, std::memory_order_relaxed);
    // release: pairs with a correlator's acquire load of the same
    // slot, so a reader that observes this tag is guaranteed to also
    // observe the sendTimeUs store above, not a stale one.
    tags_[slot].store(journalSequenceNumber, std::memory_order_release);
    // See firstSeenSeq_'s own comment: records this loop as the low
    // end of "this run's own sequence numbers", the first time it's
    // called with anything smaller than what's recorded so far.
    std::uint64_t floor = firstSeenSeq_.load(std::memory_order_relaxed);
    while ((floor == 0 || journalSequenceNumber < floor) &&
           !firstSeenSeq_.compare_exchange_weak(floor, journalSequenceNumber, std::memory_order_relaxed)) {
    }
  }

  // Prints the relay-observed round-trip percentile summary. Field
  // names are deliberately namespaced ("relay_p50_us", not "p50") so
  // this can never be mistaken by raft-tests/sweep/sweep.sh's
  // whitespace-anchored grep for the load generator's own "p50" et
  // al. — see this directory's README for why that distinction has to
  // be airtight, not just true in practice. Call after both the load
  // generator's own run() and this object's stop() have returned.
  void printSummary() const {
    std::printf("\n=== relay-observed summary ===\n");
    std::printf(
        "(submission to receipt via the relay's real gRPC stream -- not the\n"
        " synchronous ack path above; see bench/load_generator/README.md)\n");
    std::printf("relay_completed=%ld\n", static_cast<long>(observed_->total_count));
    std::printf("relay_dropped_races=%ld\n", static_cast<long>(droppedRaces_.load(std::memory_order_relaxed)));
    std::printf("relay_skipped_historical=%ld\n",
                 static_cast<long>(skippedHistorical_.load(std::memory_order_relaxed)));
    std::printf("relay_queue_high_water=%ld\n",
                 static_cast<long>(queueHighWater_.load(std::memory_order_relaxed)));
    std::printf("relay_p50_us=%ld\n", static_cast<long>(hdr_value_at_percentile(observed_, 50.0)));
    std::printf("relay_p90_us=%ld\n", static_cast<long>(hdr_value_at_percentile(observed_, 90.0)));
    std::printf("relay_p99_us=%ld\n", static_cast<long>(hdr_value_at_percentile(observed_, 99.0)));
    std::printf("relay_p99_9_us=%ld\n", static_cast<long>(hdr_value_at_percentile(observed_, 99.9)));
    std::printf("relay_max_us=%ld\n", static_cast<long>(hdr_max(observed_)));
    std::printf("relay_mean_us=%ld\n", static_cast<long>(hdr_mean(observed_)));
  }

 private:
  static constexpr std::uint64_t kUnpublished = static_cast<std::uint64_t>(-1);

  struct PendingCorrelation {
    std::uint64_t seq;
    std::int64_t arrivalUs;
  };

  static std::int64_t nowMicros() {
    return std::chrono::duration_cast<std::chrono::microseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
  }

  // The only job this thread has: keep calling Read() as fast as gRPC
  // can deliver, so a slow correlation (waitForTag() can legitimately
  // take up to 100ms) never turns into gRPC-level backpressure on the
  // stream itself — see this file's own top comment for what that
  // looked like measured live before this split existed. Parsing a
  // record and pushing to the queue is the only work done inline;
  // everything that can block moves to correlateLoop() instead.
  void readLoop() {
    gateway::relay::grpc_proto::SubscribeRequest request;
    request.set_from_sequence_number(fromSequenceNumber_);
    std::unique_ptr<grpc::ClientReader<gateway::relay::grpc_proto::RecordBatch>> reader(
        stub_->Subscribe(&context_, request));

    gateway::relay::grpc_proto::RecordBatch batch;
    while (reader->Read(&batch)) {
      // Captured once per batch, immediately after Read() returns —
      // every record a batch carries arrived at effectively the same
      // instant from this observer's point of view (see
      // relay_grpc_service_impl.hpp's own "Batching the gRPC stream"
      // comment), and this must reflect actual arrival, not how long
      // records later wait in the queue or for a correlator to pick
      // them up.
      const std::int64_t nowUs = nowMicros();

      std::vector<PendingCorrelation> pending;
      pending.reserve(static_cast<std::size_t>(batch.raw_records_size()));
      for (const std::string& rawRecord : batch.raw_records()) {
        const auto* base = reinterpret_cast<const std::byte*>(rawRecord.data());
        const journal::RecordView view(base, static_cast<std::uint32_t>(rawRecord.size()));
        pending.push_back(PendingCorrelation{view.sequenceNumber(), nowUs});
      }

      std::size_t queueSize;
      {
        std::lock_guard<std::mutex> lock(queueMutex_);
        for (const PendingCorrelation& item : pending) {
          queue_.push_back(item);
        }
        queueSize = queue_.size();
      }
      queueCv_.notify_all();
      // Relaxed max-tracking, not exact under concurrent updates from
      // multiple correlators draining at once — a diagnostic, not a
      // correctness-load-bearing count; see relay_queue_high_water's
      // own printSummary() line. A high value here means the
      // correlator pool itself is now the bottleneck (add more), a
      // low one means this split fixed the problem it was built for.
      std::int64_t prevHigh = queueHighWater_.load(std::memory_order_relaxed);
      while (static_cast<std::int64_t>(queueSize) > prevHigh &&
             !queueHighWater_.compare_exchange_weak(prevHigh, static_cast<std::int64_t>(queueSize),
                                                      std::memory_order_relaxed)) {
      }
    }
  }

  // One of correlatorThreadCount_ identical workers, each pulling the
  // next queued (sequence, arrival time) pair and running the same
  // historical-skip / bounded-wait / histogram-record logic the
  // reader thread used to run inline. Exits once the reader is done
  // (readerDone_) and the queue is empty — draining fully rather than
  // abandoning a backlog, so stop() never truncates results that were
  // already captured.
  void correlateLoop() {
    while (true) {
      PendingCorrelation item{};
      {
        std::unique_lock<std::mutex> lock(queueMutex_);
        queueCv_.wait(lock, [this] { return !queue_.empty() || readerDone_; });
        if (queue_.empty()) {
          return;  // readerDone_ and nothing left — this worker's done
        }
        item = queue_.front();
        queue_.pop_front();
      }

      const std::size_t slot = item.seq & mask_;

      // A record below the lowest sequence number recordSend() has
      // ever seen cannot possibly belong to this run — it predates
      // every request this process has made, which only happens when
      // --relay_from_sequence_number left real prior history ahead of
      // it (0, the default, always does against a journal that isn't
      // fresh). Skipping the wait for these specifically, rather than
      // treating every one as a potential race worth up to 100ms of
      // patience, is what keeps a long backlog from turning into a
      // multi-minute stall before this loop ever reaches live traffic.
      const std::uint64_t floor = firstSeenSeq_.load(std::memory_order_relaxed);
      if (floor != 0 && item.seq < floor) {
        skippedHistorical_.fetch_add(1, std::memory_order_relaxed);
        continue;
      }

      if (waitForTag(slot, item.seq)) {
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

  // The relay tails the journal directly and can deliver a record
  // before recordSend() has run for it at all — the synchronous
  // receipt that triggers recordSend() has an extra hop (through the
  // input gateway) the relay's own path does not. A bounded retry, not
  // an immediate skip: skipping unconditionally would systematically
  // under-count exactly the fastest, most interesting relay
  // deliveries. Spin first (this thread has nothing else to do while
  // waiting, and the expected wait for a genuine race is
  // microseconds), then back off; the overall bound is wall-clock time,
  // not iteration count, and deliberately generous — long enough to
  // resolve a race against a genuinely slow-tail ack too (a p99.9
  // ack is exactly the kind of sample worth keeping correlated, not
  // the kind to give up on early), not just a fast-path one. Gives up
  // only once the wait comfortably exceeds any plausible ack latency,
  // at which point either the ring is undersized or the ack genuinely
  // never arrived — either way, this one sample is dropped
  // (relay_dropped_races), not misattributed. Now called from a
  // correlator worker, not the reader thread — several of these can
  // be genuinely waiting at once, which is the whole reason more than
  // one correlator thread helps: the wait itself is wall-clock time
  // no amount of per-call optimization shortens.
  bool waitForTag(std::size_t slot, std::uint64_t seq) const {
    constexpr int kSpinIterations = 20000;  // sub-millisecond, the common-race case
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

  std::string relayGrpcAddr_;
  std::uint64_t fromSequenceNumber_;
  int correlatorThreadCount_;
  std::size_t mask_ = 0;
  std::unique_ptr<std::atomic<std::int64_t>[]> sendTimesUs_;
  std::unique_ptr<std::atomic<std::uint64_t>[]> tags_;

  struct hdr_histogram* observed_ = nullptr;
  std::atomic<std::int64_t> droppedRaces_{0};
  std::atomic<std::int64_t> skippedHistorical_{0};
  std::atomic<std::int64_t> queueHighWater_{0};
  // The lowest journal sequence number recordSend() has ever been
  // called with — 0 means "none yet". Not the ring's own tags_ (those
  // get reused within a run by design; this is a floor that, once
  // set, only ever moves down): correlateLoop() uses it to recognize
  // "predates this run entirely" without waiting on it, see there.
  std::atomic<std::uint64_t> firstSeenSeq_{0};
  std::atomic<bool> stopRequested_{false};
  std::atomic<bool> windowSet_{false};
  std::atomic<std::int64_t> measureStartUs_{0};
  std::atomic<std::int64_t> measureEndUs_{0};

  std::deque<PendingCorrelation> queue_;
  std::mutex queueMutex_;
  std::condition_variable queueCv_;
  bool readerDone_ = false;  // guarded by queueMutex_

  std::thread readerThread_;
  std::vector<std::thread> correlatorThreads_;
  std::unique_ptr<gateway::relay::grpc_proto::RelayService::Stub> stub_;
  grpc::ClientContext context_;
};

}  // namespace sequencer::bench
