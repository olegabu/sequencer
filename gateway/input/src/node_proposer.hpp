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

#include <algorithm>
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
    Bytes designatedOutput;
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
      const std::string& designated = response.designated_output();
      outcome.designatedOutput.assign(reinterpret_cast<const std::byte*>(designated.data()),
                                       reinterpret_cast<const std::byte*>(designated.data()) +
                                           designated.size());
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

    std::vector<std::shared_ptr<Pending>> batch;
    {
      std::lock_guard<std::mutex> lock(batchMutex_);
      queued_.push_back(std::move(pending));
      if (batchesInFlight_ >= maxInFlightBatches_) {
        return;  // a completing batch will pick this up
      }
      ++batchesInFlight_;
      batch = takeBatchLocked();
    }
    sendBatch(std::move(batch));
  }

 private:
  // One client request waiting for its own result inside a batch.
  struct Pending {
    Bytes input;
    std::function<void(Outcome)> onDone;
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
    return batch;
  }

  static void onBatchDone(NodeProposer* self, std::shared_ptr<BatchContext> ctx) {
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
        const std::string& designated = result.designated_output();
        outcome.designatedOutput.assign(reinterpret_cast<const std::byte*>(designated.data()),
                                         reinterpret_cast<const std::byte*>(designated.data()) +
                                             designated.size());
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
    const std::string& designated = ctx->response.designated_output();
    outcome.designatedOutput.assign(reinterpret_cast<const std::byte*>(designated.data()),
                                     reinterpret_cast<const std::byte*>(designated.data()) +
                                         designated.size());
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
