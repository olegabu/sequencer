#pragma once

// The real-gRPC counterpart to relay_service_impl.hpp — see
// proto/relay_grpc.proto's file comment for why this is a separate
// service/codegen unit rather than an extension of the brpc::Stream-
// based one.
//
// Built on gRPC's callback/reactor API (grpc::ServerWriteReactor), not
// the synchronous generated Service — see gateway/relay/README.md's
// "Batching the gRPC stream" section for why: even after batching fixed
// the original per-record Write() bottleneck, a blocking synchronous
// Write() call still ties up a dedicated OS thread for the full
// completion-queue round trip of every single call. The reactor API
// drives writes through gRPC's own callback machinery instead (this
// class's OnWriteDone), the same non-blocking-queue-and-return shape
// brpc::StreamWrite already has on the RelaySession side. Subscribe()
// itself must return immediately (the callback contract), so all the
// actual work — waiting for the journal to become readable, tailing
// it, gathering batches, writing — moves into RelaySubscribeReactor's
// own pump thread; the reactor callbacks (OnWriteDone/OnCancel/OnDone)
// only ever touch the small bit of shared state that coordinates with
// that thread.
//
// The pump thread hands write duty back to itself after every batch
// (OnWriteDone just clears a flag and notifies it) rather than
// continuing the chain directly from inside OnWriteDone. That was
// tried too — see git history — on the theory that the cross-thread
// wake/schedule round trip between OnWriteDone (a gRPC callback
// thread) and a parked pump thread was the real cost under load.
// Measured on the live fleet, it was not: p50 at 100k req/s actually
// got *worse* (1754us vs 1272us here), with no change to the ~115k
// throughput ceiling either version has (both still below the
// simpler synchronous+batched implementation's own 130k ceiling —
// see gateway/relay/README.md's "Batching the gRPC stream" section
// for the fuller writeup and the open question of why). Kept this
// simpler, better-measured version rather than the theoretically
// tidier one.
//
// Batched, not one record per Write(): each write gathers every
// record already available (up to FLAGS_relay_max_batch_records) into
// one RecordBatch — never delaying a send to wait for more to arrive.
// Caught up, this degrades to exactly one record per batch; behind, it
// collapses however large the backlog is into far fewer Write() round
// trips.
//
// Idle waiting is spin-then-backoff (the same idiom
// bench/load_generator's own RelayObserver::waitForTag already uses),
// not a flat poll interval: a flat 5ms sleep puts a real latency floor
// under every delivery caught up to the journal's own tail.
//
// No IsCancelled() polling anywhere, on purpose — the bug this once
// had (see git history: a stray per-record IsCancelled() call inside
// the gather loop, ~3000x slower, since IsCancelled() plucks gRPC's own
// completion queue under the hood) simply has no equivalent here:
// OnCancel() is invoked directly by gRPC when the client disconnects,
// so there is nothing to poll for at all.

#include <gflags/gflags.h>
#include <grpcpp/grpcpp.h>

#include "relay_gateway_impl.hpp"
#include "relay_grpc.grpc.pb.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <thread>

// ~1024 x ~50 bytes ≈ 50KB per batch at counter-record sizes, comfortably
// under gRPC's 4MB default message-size cap; tune per deployment if
// records are much larger. Included in exactly one translation unit per
// binary (run_relay_gateway.cpp, relay_grpc_test.cpp — separate
// executables), so defining the flag directly in this header is safe.
DEFINE_int32(relay_max_batch_records, 1024,
             "Max journal records the relay's gRPC Subscribe stream gathers into one "
             "RecordBatch/Write() call when a backlog exists. Never delays a send to "
             "accumulate a batch — caught up, every batch is exactly 1 record.");

namespace sequencer::gateway::relay::detail {

// One instance per active Subscribe() call, owned by gRPC itself once
// returned from RelayGrpcServiceImpl::Subscribe (self-deletes in
// OnDone(), the standard reactor-lifetime pattern). `serviceStopping` is
// a reference to the service's own flag, not a copy — safe because the
// service is guaranteed to outlive every reactor it creates: the
// existing shutdown sequence (requestStop(); server->Shutdown(deadline);
// server->Wait();) blocks in Wait() until every outstanding call's
// OnDone() has already fired before the service itself can be
// destroyed.
class RelaySubscribeReactor final : public ::grpc::ServerWriteReactor<grpc_proto::RecordBatch> {
 public:
  RelaySubscribeReactor(RelayGatewayImpl& gateway, std::uint64_t fromSequenceNumber,
                         std::atomic<bool>& serviceStopping)
      : gateway_(gateway),
        nextSeq_(fromSequenceNumber == 0 ? 1 : fromSequenceNumber),
        serviceStopping_(serviceStopping) {
    pumpThread_ = std::thread([this] { pumpLoop(); });
  }

  void OnWriteDone(bool ok) override {
    if (!ok) {
      stopped_.store(true, std::memory_order_relaxed);
    }
    {
      std::lock_guard<std::mutex> lock(mutex_);
      writeInFlight_ = false;
    }
    cv_.notify_all();
  }

  void OnCancel() override {
    stopped_.store(true, std::memory_order_relaxed);
    cv_.notify_all();
  }

  // Standard reactor-lifetime pattern: OnDone() is gRPC's own signal
  // that Finish() has fully completed and this call is over — the
  // pump thread (which is the only thing that ever calls Finish(), at
  // the very end of pumpLoop()) has therefore already returned or is
  // about to; join() here is never a self-join and never blocks long.
  void OnDone() override {
    stopped_.store(true, std::memory_order_relaxed);
    cv_.notify_all();
    if (pumpThread_.joinable()) {
      pumpThread_.join();
    }
    delete this;
  }

 private:
  bool stopRequested() const {
    return stopped_.load(std::memory_order_relaxed) || serviceStopping_.load(std::memory_order_relaxed);
  }

  void pumpLoop() {
    const std::shared_ptr<journal::JournalReader> reader = gateway_.waitForReader(std::chrono::seconds(5));
    if (!reader) {
      Finish(::grpc::Status(::grpc::StatusCode::UNAVAILABLE, "journal not yet available"));
      return;
    }

    const int maxBatch = std::max(1, FLAGS_relay_max_batch_records);
    // Sub-millisecond common case; falls back to a short sleep only
    // once genuinely idle (nothing new has landed in the journal).
    constexpr int kSpinIterations = 20000;
    int spins = 0;
    while (!stopRequested()) {
      if (!reader->contains(nextSeq_)) {
        if (spins < kSpinIterations) {
          ++spins;
          continue;
        }
        std::this_thread::sleep_for(std::chrono::microseconds(200));
        continue;
      }
      spins = 0;

      batch_.Clear();
      for (int gathered = 0; gathered < maxBatch && reader->contains(nextSeq_) && !stopRequested();
           ++gathered, ++nextSeq_) {
        const journal::RecordView view = reader->record(nextSeq_);
        const Payload raw = view.rawBytes();
        batch_.add_raw_records(raw.data(), raw.size());
      }
      if (batch_.raw_records_size() == 0) {
        continue;  // stopped mid-gather, nothing to send
      }

      {
        std::lock_guard<std::mutex> lock(mutex_);
        writeInFlight_ = true;
      }
      StartWrite(&batch_);
      {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [this] { return !writeInFlight_ || stopRequested(); });
      }
    }

    // Safe to call here even with a write still technically
    // outstanding (measured: after a client cancels mid-write, gRPC
    // does not reliably deliver OnWriteDone promptly — waiting for it
    // here first, as an earlier version of this method did, added a
    // multi-hundred-millisecond stall to every cancellation). Finish()
    // with a pending write is an explicitly supported "early finish":
    // gRPC still delivers that write's OnWriteDone internally before
    // OnDone() fires, in the right order, without this method needing
    // to wait for it first.
    Finish(::grpc::Status::OK);
  }

  RelayGatewayImpl& gateway_;
  std::uint64_t nextSeq_;
  std::atomic<bool>& serviceStopping_;
  std::atomic<bool> stopped_{false};
  grpc_proto::RecordBatch batch_;  // touched only by pumpLoop(), never concurrently with OnWriteDone
  std::mutex mutex_;
  std::condition_variable cv_;
  bool writeInFlight_ = false;  // guarded by mutex_
  std::thread pumpThread_;
};

class RelayGrpcServiceImpl final : public grpc_proto::RelayService::CallbackService {
 public:
  explicit RelayGrpcServiceImpl(RelayGatewayImpl& gateway) : gateway_(gateway) {}

  // Called right before grpc::Server::Shutdown(): an idle subscriber's
  // reactor has nothing else to wake it — there's no more data coming
  // and no client cancellation either — so without this, shutdown
  // would stall for the caller's full Shutdown() deadline. Every
  // active reactor's pump thread checks this (via stopRequested()) at
  // least as often as its own spin/backoff cadence, so this makes
  // shutdown near-instant instead; Shutdown()'s own deadline becomes
  // just a backstop, the same "belt and suspenders" relationship
  // grpc_output_transport.cpp's closeAll() has with its own Shutdown().
  void requestStop() { stopRequested_.store(true, std::memory_order_relaxed); }

  ::grpc::ServerWriteReactor<grpc_proto::RecordBatch>* Subscribe(
      ::grpc::CallbackServerContext* /*context*/, const grpc_proto::SubscribeRequest* request) override {
    return new RelaySubscribeReactor(gateway_, request->from_sequence_number(), stopRequested_);
  }

 private:
  RelayGatewayImpl& gateway_;
  std::atomic<bool> stopRequested_{false};
};

}  // namespace sequencer::gateway::relay::detail
