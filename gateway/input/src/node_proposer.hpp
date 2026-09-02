#pragma once

// The "proposeToLeader" step of specification.md §8.6's chassis loop:
// submit to the raft group's current leader, following redirects
// exactly as specification.md §8.1 requires ("forward to the current
// leader, follow redirects, and on timeout resubmit blindly"). Caches
// the last-known leader across calls — unlike a one-shot test helper,
// a real gateway handles many requests and shouldn't rediscover the
// leader from scratch every time.
//
// It caches the *channels* too, one per endpoint. An earlier version
// cached only the leader's address and built a fresh brpc::Channel
// inside every propose() call. brpc::Channel is explicitly safe to
// share across threads and is meant to be long-lived (it pools sockets
// internally), so building one per request was paying setup cost for
// nothing.
//
// Worth being precise about how much that was worth, because it is
// less than it looks: measured at 100k req/s, caching moved this
// gateway's p50 from ~1351-1367us to ~1311-1335us — about 30us, 2-3%.
// It is a real improvement and simply the correct way to use a
// Channel, but it is NOT the explanation for the gap against
// submitting straight to a node (~727us at the same rate). See
// raft-tests/sequencer/README.md's "What the input gateway costs" for
// where that gap actually comes from.

#include <brpc/channel.h>
#include <brpc/controller.h>
#include <bvar/bvar.h>
#include <butil/time.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <functional>
#include <iterator>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <sequencer/payload.hpp>

#include "node.pb.h"

namespace sequencer::gateway::input::detail {

// Instrumentation for the batching path, exposed on the gateway's own
// brpc /vars page (brpc publishes every bvar there automatically, so
// this needs no wiring beyond declaring them).
//
// It exists to answer one question that reasoning could not settle:
// sweeps found a throughput ceiling near 380k req/s that is not CPU on
// any box -- the raft leader measured 38.7% busy at 350k on 16 cores,
// and the client boxes were not saturated either. The batching bounds
// below are the other candidate, since maxInFlightBatches_ is a hard
// cap on how many proposals can be on the wire at once, and a proposal
// arriving when every slot is busy simply waits. `proposals_deferred`
// counts exactly those. If it stays near zero as the offered rate
// approaches the ceiling, this cap is not the ceiling and the search
// moves elsewhere; if it climbs with the ceiling, it is.
//
// The gauges are read through free functions rather than members so
// that bvar's sampling callback never re-enters the singleton's own
// initialization.
inline std::atomic<int>& queueDepthGauge() {
  static std::atomic<int> value{0};
  return value;
}
inline std::atomic<int>& batchesInFlightGauge() {
  static std::atomic<int> value{0};
  return value;
}

struct ProposerMetrics {
  static ProposerMetrics& instance() {
    // One set of bvars per process regardless of how many NodeProposers
    // exist: bvar names are process-global, and tests construct several.
    static ProposerMetrics metrics;
    return metrics;
  }

  // A proposal that arrived with every in-flight slot busy, and so had
  // to wait for a batch to come back before it could go anywhere.
  bvar::Adder<std::uint64_t> deferred{"input_gateway_proposals_deferred"};
  // Enqueue-to-wire, per proposal. Distinguishes "waiting for a slot"
  // from "waiting for the raft group", which the end-to-end latency
  // alone cannot separate.
  bvar::LatencyRecorder queueDelayUs{"input_gateway_batch_queue_delay_us"};
  // What batch sizes actually go out. The cap is 64; if the typical
  // batch is far below it under load, batch size is not binding.
  //
  // Windowed, NOT a bare IntRecorder: that publishes a LIFETIME
  // average, so under a rising sweep every reading is dominated by all
  // the low-rate traffic that came before it. The first run of this
  // instrumentation reported an average batch size of 1.0 at every rate
  // up to 300k for exactly that reason, which is not what was happening
  // -- the same run's own numbers imply batches of 3-4 there.
  bvar::IntRecorder batchSizeRaw;
  bvar::Window<bvar::IntRecorder> batchSize{"input_gateway_batch_size", &batchSizeRaw, -1};
  // The other half of the split queueDelayUs exists to make: once a
  // batch is on the wire, how long the raft group takes to commit it
  // and answer. queueDelayUs says "waiting for a slot"; this says
  // "waiting for the group", and the two together account for a
  // proposal's whole life inside the gateway.
  //
  // Added while chasing a tail that survives the journal fix: the apply
  // thread, braft's append-entry RPCs, braft's own log roll, device
  // writeback and this queue were each measured and each excluded, all
  // of them well under the 8-60ms the client sees. This was the one
  // span left unmeasured.
  bvar::LatencyRecorder rpcLatencyUs{"input_gateway_propose_rpc_us"};
  bvar::Maxer<int> queueDepthMax{"input_gateway_queue_depth_max"};
  bvar::PassiveStatus<int> queueDepthNow{"input_gateway_queue_depth", readQueueDepth, nullptr};
  bvar::PassiveStatus<int> batchesInFlightNow{"input_gateway_batches_in_flight",
                                               readBatchesInFlight, nullptr};

 private:
  static int readQueueDepth(void*) {
    return queueDepthGauge().load(std::memory_order_relaxed);
  }
  static int readBatchesInFlight(void*) {
    return batchesInFlightGauge().load(std::memory_order_relaxed);
  }
};


class NodeProposer {
 public:
  // maxBatchSize/maxInFlightBatches: see kMaxBatchSize's own comment
  // below for what each bounds. 0 keeps the default.
  explicit NodeProposer(std::vector<std::string> nodeEndpoints, std::size_t maxBatchSize = 0,
                         int maxInFlightBatches = 0)
      : maxBatchSize_(maxBatchSize > 0 ? maxBatchSize : kMaxBatchSize),
        maxInFlightBatches_(maxInFlightBatches > 0 ? maxInFlightBatches : kMaxInFlightBatches),
        endpoints_(std::move(nodeEndpoints)) {}

  struct Outcome {
    bool ok = false;
    Receipt receipt{};
    std::vector<Bytes> designatedOutputs;
    std::string errorMessage;
  };

  Outcome propose(Payload input) {
    std::string target = currentTarget();
    std::size_t endpointIndex = 0;
    // Generous but bounded: one hop per known endpoint, plus a couple
    // of extra attempts to follow a chain of redirects without looping
    // forever against a group that can't currently elect a leader.
    const int maxAttempts = static_cast<int>(endpoints_.size()) + 3;

    for (int attempt = 0; attempt < maxAttempts; ++attempt) {
      const std::shared_ptr<brpc::Channel> channel = channelFor(target);
      if (channel == nullptr) {
        target = nextEndpoint(endpointIndex);
        continue;
      }

      sequencer::node::proto::ProposeService_Stub stub(channel.get());
      sequencer::node::proto::ProposeRequest request;
      request.set_input(input.data(), input.size());
      sequencer::node::proto::ProposeResponse response;
      brpc::Controller cntl;
      stub.Propose(&cntl, &request, &response, nullptr);

      if (cntl.Failed()) {
        target = nextEndpoint(endpointIndex);
        continue;
      }
      if (response.redirect()) {
        target = response.leader_hint().empty() ? nextEndpoint(endpointIndex)
                                                  : stripPeerIdIndex(response.leader_hint());
        continue;
      }
      if (!response.error_message().empty()) {
        target = nextEndpoint(endpointIndex);
        continue;
      }

      rememberLeader(target);
      Outcome outcome;
      outcome.ok = true;
      outcome.receipt.sequenceNumber = response.sequence_number();
      for (const std::string& designated : response.designated_outputs()) {
        outcome.designatedOutputs.emplace_back(
            reinterpret_cast<const std::byte*>(designated.data()),
            reinterpret_cast<const std::byte*>(designated.data()) + designated.size());
      }
      return outcome;
    }

    Outcome outcome;
    outcome.ok = false;
    outcome.errorMessage = "failed to reach the raft group's leader after " +
                            std::to_string(maxAttempts) + " attempt(s)";
    return outcome;
  }

  // The same thing without occupying the caller for the node's round
  // trip: issues the Propose asynchronously and invokes `onDone` from
  // brpc's callback, following redirects by re-issuing rather than by
  // looping in place.
  //
  // This exists because the synchronous form above holds a brpc worker
  // for the whole cross-AZ round trip, and an input gateway serves one
  // request per proposal — so its capacity became (workers / RTT)
  // rather than anything about the raft group. Measured at 100k req/s,
  // raising the gateway's worker count alone moved p50 from 1320us to
  // 891us, which is the signature of exactly that; making the handler
  // async removes the ceiling instead of raising it. See
  // raft-tests/sequencer/README.md's "What the input gateway costs".
  //
  // `input` is copied: the caller's buffer is a request-scoped local
  // and this call outlives it.
  //
  // Proposals are BATCHED onto the wire, opportunistically: this
  // enqueues and, if there is a free in-flight slot, immediately sends
  // whatever is queued (up to kMaxBatchSize) as one ProposeBatch. It
  // never waits for a batch to fill — a caller that arrives alone is
  // sent alone, so low-rate latency is unchanged.
  //
  // BOTH bounds are load-bearing, and the first version had neither.
  // Allowing only ONE batch in flight measured 133ms p50 and dropped
  // throughput to 75k: a single outstanding request-response pair
  // caps throughput at batch/RTT, which had previously been fully
  // pipelined across every in-flight client request. Leaving batch
  // size unbounded then let each batch swell to ~10k inputs, and a
  // batch's latency is its SLOWEST member, so everyone in it waited
  // for all of it. Several modest batches in flight keeps the pipe
  // full and keeps any one client's wait short.
  //
  // This is where the analogy to gateway/output stops: those batched
  // writes are fire-and-forget, so one in flight costs nothing. A
  // proposal has a round trip, so concurrency has to be preserved.
  //
  // This is the same discipline gateway/relay and gateway/output both
  // arrived at — "gather what's available now, send once, never delay
  // to wait for more" — and it is here for the same measured reason:
  // profiling this gateway at 100k found a flat profile of socket
  // syscalls, kernel spinlocks and thread wakeups, with no application
  // symbol near the top. One Propose RPC per client request was the
  // last unbatched hot path in the system.
  void proposeAsync(Payload input, std::function<void(Outcome)> onDone) {
    auto pending = std::make_shared<Pending>();
    pending->input.assign(input.begin(), input.end());
    pending->onDone = std::move(onDone);
    pending->enqueuedUs = butil::cpuwide_time_us();

    std::vector<std::shared_ptr<Pending>> batch;
    {
      std::lock_guard<std::mutex> lock(batchMutex_);
      queued_.push_back(std::move(pending));
      if (batchesInFlight_ >= maxInFlightBatches_) {
        // Every slot is busy: this proposal waits for a batch to come
        // back. Counting these is the whole point of the metrics --
        // see ProposerMetrics.
        ProposerMetrics::instance().deferred << 1;
        publishQueueDepthLocked();
        return;  // a completing batch will pick this up
      }
      ++batchesInFlight_;
      batchesInFlightGauge().store(batchesInFlight_, std::memory_order_relaxed);
      batch = takeBatchLocked();
    }
    sendBatch(std::move(batch));
  }

 private:
  // One client request waiting for its own result inside a batch.
  struct Pending {
    Bytes input;
    std::function<void(Outcome)> onDone;
    // For input_gateway_batch_queue_delay_us; see ProposerMetrics.
    std::int64_t enqueuedUs = 0;
  };

  struct BatchContext {
    std::vector<std::shared_ptr<Pending>> pendings;
    std::string target;
    int attemptsLeft = 0;
    std::size_t endpointIndex = 0;
    brpc::Controller cntl;
    sequencer::node::proto::ProposeBatchRequest request;
    sequencer::node::proto::ProposeBatchResponse response;
  };

  void sendBatch(std::vector<std::shared_ptr<Pending>> pendings) {
    auto ctx = std::make_shared<BatchContext>();
    ctx->pendings = std::move(pendings);
    ctx->target = currentTarget();
    ctx->attemptsLeft = static_cast<int>(endpoints_.size()) + 3;
    issueBatch(ctx);
  }

  void issueBatch(const std::shared_ptr<BatchContext>& ctx) {
    if (ctx->attemptsLeft-- <= 0) {
      finishBatch(ctx, "failed to reach the raft group's leader after every attempt");
      return;
    }
    const std::shared_ptr<brpc::Channel> channel = channelFor(ctx->target);
    if (channel == nullptr) {
      ctx->target = nextEndpoint(ctx->endpointIndex);
      issueBatch(ctx);
      return;
    }
    ctx->cntl.Reset();
    ctx->response.Clear();
    ctx->request.Clear();
    for (const std::shared_ptr<Pending>& p : ctx->pendings) {
      ctx->request.add_inputs(p->input.data(), p->input.size());
    }
    sequencer::node::proto::ProposeService_Stub stub(channel.get());
    stub.ProposeBatch(&ctx->cntl, &ctx->request, &ctx->response,
                       brpc::NewCallback(&NodeProposer::onBatchDone, this, ctx));
  }

  // Hands every pending in this batch the same failure, then releases
  // the in-flight slot so whatever queued meanwhile can go.
  void finishBatch(const std::shared_ptr<BatchContext>& ctx, const std::string& message) {
    for (const std::shared_ptr<Pending>& p : ctx->pendings) {
      Outcome outcome;
      outcome.ok = false;
      outcome.errorMessage = message;
      p->onDone(outcome);
    }
    releaseAndDrain();
  }

  // A completing batch immediately takes whatever accumulated while it
  // was out, keeping its slot. That is what makes batch size track
  // load without any timer: idle means batches of one, busy means
  // batches up to kMaxBatchSize.
  void releaseAndDrain() {
    std::vector<std::shared_ptr<Pending>> next;
    {
      std::lock_guard<std::mutex> lock(batchMutex_);
      if (queued_.empty()) {
        --batchesInFlight_;
        batchesInFlightGauge().store(batchesInFlight_, std::memory_order_relaxed);
        return;
      }
      // Keep this slot rather than releasing and re-acquiring it.
      next = takeBatchLocked();
    }
    sendBatch(std::move(next));
  }

  // Caller must hold batchMutex_.
  std::vector<std::shared_ptr<Pending>> takeBatchLocked() {
    const std::size_t take = std::min(queued_.size(), maxBatchSize_);
    std::vector<std::shared_ptr<Pending>> batch(
        std::make_move_iterator(queued_.begin()),
        std::make_move_iterator(queued_.begin() + static_cast<std::ptrdiff_t>(take)));
    queued_.erase(queued_.begin(), queued_.begin() + static_cast<std::ptrdiff_t>(take));
    publishQueueDepthLocked();
    ProposerMetrics::instance().batchSizeRaw << static_cast<int>(batch.size());
    const std::int64_t nowUs = butil::cpuwide_time_us();
    for (const std::shared_ptr<Pending>& p : batch) {
      ProposerMetrics::instance().queueDelayUs << (nowUs - p->enqueuedUs);
    }
    return batch;
  }

  // Caller must hold batchMutex_.
  void publishQueueDepthLocked() {
    const int depth = static_cast<int>(queued_.size());
    queueDepthGauge().store(depth, std::memory_order_relaxed);
    ProposerMetrics::instance().queueDepthMax << depth;
  }

  static void onBatchDone(NodeProposer* self, std::shared_ptr<BatchContext> ctx) {
    // brpc's own measurement of the call, so it covers the wire, the
    // node's server queue, consensus and the reply. Recorded before the
    // retry paths below, which reissue on a fresh Controller.
    if (!ctx->cntl.Failed()) {
      ProposerMetrics::instance().rpcLatencyUs << ctx->cntl.latency_us();
    }
    if (ctx->cntl.Failed()) {
      ctx->target = self->nextEndpoint(ctx->endpointIndex);
      self->issueBatch(ctx);
      return;
    }
    if (ctx->response.redirect()) {
      // Nothing in the batch was applied (see node.proto), so the whole
      // batch is safe to resend at the new leader.
      ctx->target = ctx->response.leader_hint().empty()
                        ? self->nextEndpoint(ctx->endpointIndex)
                        : stripPeerIdIndex(ctx->response.leader_hint());
      self->issueBatch(ctx);
      return;
    }
    if (ctx->response.results_size() != static_cast<int>(ctx->pendings.size())) {
      self->finishBatch(ctx, "node returned a result count that does not match the batch");
      return;
    }

    self->rememberLeader(ctx->target);
    for (std::size_t i = 0; i < ctx->pendings.size(); ++i) {
      const sequencer::node::proto::ProposeResult& result = ctx->response.results(static_cast<int>(i));
      Outcome outcome;
      if (!result.error_message().empty()) {
        outcome.ok = false;
        outcome.errorMessage = result.error_message();
      } else {
        outcome.ok = true;
        outcome.receipt.sequenceNumber = result.sequence_number();
        for (const std::string& designated : result.designated_outputs()) {
          outcome.designatedOutputs.emplace_back(
              reinterpret_cast<const std::byte*>(designated.data()),
              reinterpret_cast<const std::byte*>(designated.data()) + designated.size());
        }
      }
      ctx->pendings[i]->onDone(std::move(outcome));
    }
    self->releaseAndDrain();
  }

  struct AsyncContext {
    Bytes input;
    std::function<void(Outcome)> onDone;
    std::string target;
    int attemptsLeft = 0;
    std::size_t endpointIndex = 0;
    // One per attempt, recreated by issue(); brpc requires these to
    // outlive the call, which the shared_ptr guarantees.
    brpc::Controller cntl;
    sequencer::node::proto::ProposeRequest request;
    sequencer::node::proto::ProposeResponse response;
  };

  void fail(const std::shared_ptr<AsyncContext>& ctx, const std::string& message) {
    Outcome outcome;
    outcome.ok = false;
    outcome.errorMessage = message;
    ctx->onDone(std::move(outcome));
  }

  void issue(const std::shared_ptr<AsyncContext>& ctx) {
    if (ctx->attemptsLeft-- <= 0) {
      fail(ctx, "failed to reach the raft group's leader after every attempt");
      return;
    }
    const std::shared_ptr<brpc::Channel> channel = channelFor(ctx->target);
    if (channel == nullptr) {
      ctx->target = nextEndpoint(ctx->endpointIndex);
      issue(ctx);
      return;
    }
    ctx->cntl.Reset();
    ctx->response.Clear();
    ctx->request.set_input(ctx->input.data(), ctx->input.size());
    sequencer::node::proto::ProposeService_Stub stub(channel.get());
    // The lambda keeps `ctx` alive until the call completes; brpc runs
    // it on one of its own threads once the response (or failure)
    // lands.
    stub.Propose(&ctx->cntl, &ctx->request, &ctx->response,
                  brpc::NewCallback(&NodeProposer::onProposeDone, this, ctx));
  }

  static void onProposeDone(NodeProposer* self, std::shared_ptr<AsyncContext> ctx) {
    if (ctx->cntl.Failed()) {
      ctx->target = self->nextEndpoint(ctx->endpointIndex);
      self->issue(ctx);
      return;
    }
    if (ctx->response.redirect()) {
      ctx->target = ctx->response.leader_hint().empty()
                        ? self->nextEndpoint(ctx->endpointIndex)
                        : stripPeerIdIndex(ctx->response.leader_hint());
      self->issue(ctx);
      return;
    }
    if (!ctx->response.error_message().empty()) {
      ctx->target = self->nextEndpoint(ctx->endpointIndex);
      self->issue(ctx);
      return;
    }

    self->rememberLeader(ctx->target);
    Outcome outcome;
    outcome.ok = true;
    outcome.receipt.sequenceNumber = ctx->response.sequence_number();
    for (const std::string& designated : ctx->response.designated_outputs()) {
      outcome.designatedOutputs.emplace_back(
          reinterpret_cast<const std::byte*>(designated.data()),
          reinterpret_cast<const std::byte*>(designated.data()) + designated.size());
    }
    ctx->onDone(std::move(outcome));
  }

  static std::string stripPeerIdIndex(const std::string& peerId) {
    const auto lastColon = peerId.rfind(':');
    return lastColon == std::string::npos ? peerId : peerId.substr(0, lastColon);
  }

  std::string currentTarget() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (cachedLeader_.has_value()) {
      return *cachedLeader_;
    }
    return endpoints_.front();
  }

  std::string nextEndpoint(std::size_t& index) {
    std::lock_guard<std::mutex> lock(mutex_);
    cachedLeader_.reset();
    const std::string& endpoint = endpoints_[index % endpoints_.size()];
    ++index;
    return endpoint;
  }

  void rememberLeader(const std::string& target) {
    std::lock_guard<std::mutex> lock(mutex_);
    cachedLeader_ = target;
  }

  // One long-lived channel per endpoint, shared by every request —
  // see this file's own header comment for what building one per
  // request cost. Returns nullptr if this endpoint can't be
  // initialized, which the caller treats as "try the next one".
  std::shared_ptr<brpc::Channel> channelFor(const std::string& target) {
    {
      std::lock_guard<std::mutex> lock(channelsMutex_);
      const auto it = channels_.find(target);
      if (it != channels_.end()) {
        return it->second;
      }
    }
    auto channel = std::make_shared<brpc::Channel>();
    brpc::ChannelOptions channelOptions;
    channelOptions.timeout_ms = 2000;
    channelOptions.max_retry = 0;
    if (channel->Init(target.c_str(), &channelOptions) != 0) {
      return nullptr;
    }
    std::lock_guard<std::mutex> lock(channelsMutex_);
    // Another thread may have raced us here; either instance is fine,
    // so keep whichever landed first and let ours go.
    const auto [it, inserted] = channels_.emplace(target, std::move(channel));
    return it->second;
  }

  // Chosen by sweeping both at 40k/100k/130k on a live 3-node fleet,
  // not derived. The two bounds pull against each other: more
  // in-flight slots means each batch is smaller, which costs less at
  // low load (less grouping, so nobody waits on batch-mates) and wins
  // less at high load (fewer syscalls saved). Measured p50, batch
  // size 64 throughout:
  //
  //   slots      40k     100k     130k
  //       8      763     1109     1348
  //      16      702     1026     1316
  //      32      640        -     1557
  //
  // 16 is better than 8 at every rate measured and better than 32
  // where it matters; batch size 256 was clearly worse than 64 (1714
  // against 1009 at 100k), since a batch's latency is its slowest
  // member. Override with --max_batch_size / --max_inflight_batches.
  //
  // IMPORTANT, and not visible from a single gateway: that sweep ran
  // ONE gateway. This bound is per-gateway, but what the leader feels
  // is the SUM across every gateway proposing to it. Five gateways at
  // 16 apiece put 80 concurrent batches on a leader that does its best
  // work near 16-20, and the leader's own apply latency degrades under
  // that pressure. Re-swept with five gateways -- end-to-end p50, and
  // where the cluster stops keeping up:
  //
  //   slots    100k    300k    350k    400k       450k    500k
  //       4     803    1509    1654    2268       3204    collapsed
  //       8     680    1268    1568    collapsed     -        -
  //      16     683    1324    1750    collapsed     -        -
  //      64     687    2070    collapsed   -          -        -
  //
  // The product (gateways x slots) is what matters, not slots alone:
  // 5x4 = 20 lands near the total concurrency 1x16 already had, and it
  // is the only setting here that carries 450k. Raising the bound is
  // actively harmful -- 64 collapses earliest, because removing the
  // backpressure just moves the queue onto the leader.
  //
  // So the deferral this bound causes is not waste, it is admission
  // control, and a large input_gateway_proposals_deferred is a sign it
  // is working rather than something to tune away. That was established
  // by predicting the opposite and being wrong: the first hypothesis
  // was that deferral WAS the ceiling, and raising the bound to 64 to
  // relieve it made every rate worse and moved the knee down from
  // ~360k to ~330k.
  //
  // Rule of thumb: divide roughly 16-20 by the number of input gateways
  // feeding one leader, floored at 1. Low rates prefer the larger value
  // (fewer proposals wait for a slot), high rates need the smaller one,
  // so choose for the load you must survive rather than the load you
  // usually see.
  static constexpr std::size_t kMaxBatchSize = 64;
  static constexpr int kMaxInFlightBatches = 16;

  const std::size_t maxBatchSize_;
  const int maxInFlightBatches_;

  std::mutex batchMutex_;
  std::vector<std::shared_ptr<Pending>> queued_;
  int batchesInFlight_ = 0;

  std::mutex channelsMutex_;
  std::unordered_map<std::string, std::shared_ptr<brpc::Channel>> channels_;
  std::vector<std::string> endpoints_;
  std::mutex mutex_;
  std::optional<std::string> cachedLeader_;
};

}  // namespace sequencer::gateway::input::detail
