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

#include <functional>
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
  explicit NodeProposer(std::vector<std::string> nodeEndpoints) : endpoints_(std::move(nodeEndpoints)) {}

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
  void proposeAsync(Payload input, std::function<void(Outcome)> onDone) {
    auto ctx = std::make_shared<AsyncContext>();
    ctx->input.assign(input.begin(), input.end());
    ctx->onDone = std::move(onDone);
    ctx->target = currentTarget();
    ctx->attemptsLeft = static_cast<int>(endpoints_.size()) + 3;
    issue(ctx);
  }

 private:
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

  std::mutex channelsMutex_;
  std::unordered_map<std::string, std::shared_ptr<brpc::Channel>> channels_;
  std::vector<std::string> endpoints_;
  std::mutex mutex_;
  std::optional<std::string> cachedLeader_;
};

}  // namespace sequencer::gateway::input::detail
