#pragma once

// The "proposeToLeader" step of specification.md §8.6's chassis loop:
// submit to the raft group's current leader, following redirects
// exactly as specification.md §8.1 requires ("forward to the current
// leader, follow redirects, and on timeout resubmit blindly"). Caches
// the last-known leader across calls — unlike a one-shot test helper,
// a real gateway handles many requests and shouldn't rediscover the
// leader from scratch every time.

#include <brpc/channel.h>
#include <brpc/controller.h>

#include <mutex>
#include <optional>
#include <string>
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
      brpc::Channel channel;
      brpc::ChannelOptions channelOptions;
      channelOptions.timeout_ms = 2000;
      channelOptions.max_retry = 0;
      if (channel.Init(target.c_str(), &channelOptions) != 0) {
        target = nextEndpoint(endpointIndex);
        continue;
      }

      sequencer::node::proto::ProposeService_Stub stub(&channel);
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

 private:
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

  std::vector<std::string> endpoints_;
  std::mutex mutex_;
  std::optional<std::string> cachedLeader_;
};

}  // namespace sequencer::gateway::input::detail
