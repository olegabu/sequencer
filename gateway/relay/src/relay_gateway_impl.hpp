#pragma once

// specification.md §8.2: "run colocated with exactly one replica,
// reading that replica's journal via local memory-map only; re-serve
// Subscribe(fromSequenceNumber) over the network with byte-identical
// records; be attachable to any replica, leader or follower (§3.3);
// support any number of concurrent remote subscribers."
//
// RelayGatewayImpl owns exactly one shared, colocated JournalReader
// (opened lazily — the node may start after the relay does, matching
// gateway/output's OutputGatewayImpl and evidence/'s
// SigningGatewayImpl) and a set of independent RelaySessions, one per
// active Subscribe call, each reading that same reader concurrently —
// safe and cheap, since JournalReader is exactly the "any number of
// independent, concurrent readers" type journal/ was built for (§6.4).

#include <atomic>
#include <chrono>
#include <filesystem>
#include <map>
#include <memory>
#include <mutex>
#include <thread>

#include <brpc/controller.h>
#include <brpc/stream.h>

#include <sequencer/journal/reader.hpp>

#include "relay.pb.h"
#include "relay_session.hpp"

namespace sequencer::gateway::relay::detail {

struct RelayGatewayConfig {
  std::filesystem::path dataDir;  // a node's journal directory, colocated (§3, §8.2)
  int listenPort = 0;
};

class RelayGatewayImpl {
 public:
  explicit RelayGatewayImpl(RelayGatewayConfig config) : config_(std::move(config)) {}

  RelayGatewayImpl(const RelayGatewayImpl&) = delete;
  RelayGatewayImpl& operator=(const RelayGatewayImpl&) = delete;

  ~RelayGatewayImpl() {
    if (started_) {
      stop();
    }
  }

  void start() {
    maintenanceThread_ = std::thread([this] { maintenanceLoop(); });
    started_ = true;
  }

  void stop() {
    stopRequested_.store(true, std::memory_order_relaxed);
    if (maintenanceThread_.joinable()) {
      maintenanceThread_.join();
    }
    // Every remaining session's destructor closes its stream and waits
    // for confirmed closure (see relay_session.hpp) before returning.
    std::lock_guard<std::mutex> lock(mutex_);
    sessions_.clear();
    started_ = false;
  }

  // Called by RelayServiceImpl's Subscribe handler. Accepts the
  // client's stream and starts a new RelaySession for it. Returns
  // false (with `errorMessage` set) if the journal isn't readable yet
  // or the stream couldn't be accepted.
  bool startSession(brpc::Controller& cntl, std::uint64_t fromSequenceNumber, std::string* errorMessage) {
    std::shared_ptr<journal::JournalReader> reader = currentReader();
    if (!reader) {
      *errorMessage = "journal not yet available";
      return false;
    }

    auto session = std::make_unique<RelaySession>(reader, fromSequenceNumber);
    brpc::StreamId streamId = brpc::INVALID_STREAM_ID;
    brpc::StreamOptions options;
    options.handler = session.get();
    if (brpc::StreamAccept(&streamId, cntl, &options) != 0) {
      *errorMessage = "StreamAccept failed";
      return false;
    }
    session->start(streamId);

    std::lock_guard<std::mutex> lock(mutex_);
    sessions_[streamId] = std::move(session);
    return true;
  }

  int listenPort() const { return config_.listenPort; }

  // Test/observability convenience: how many sessions are currently
  // tracked (open or not-yet-reaped-closed).
  std::size_t sessionCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return sessions_.size();
  }

 private:
  std::shared_ptr<journal::JournalReader> currentReader() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return reader_;
  }

  void maintenanceLoop() {
    while (!stopRequested_.load(std::memory_order_relaxed)) {
      if (!currentReader()) {
        try {
          auto reader = std::make_shared<journal::JournalReader>(config_.dataDir / "journal.data",
                                                                   config_.dataDir / "journal.index");
          std::lock_guard<std::mutex> lock(mutex_);
          reader_ = reader;
        } catch (const std::exception&) {
          // The journal file pair may not exist yet — retry.
        }
      }
      reapClosedSessions();
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
  }

  // Closed sessions are only ever destroyed from here, never from
  // inside on_closed() itself — on_closed() just flips a flag
  // (RelaySession::isClosed()); actually erasing (and thus destroying)
  // the session happens later, from this single maintenance thread,
  // avoiding any re-entrant callback-into-gateway-from-inside-brpc's-
  // own-callback complexity.
  void reapClosedSessions() {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto it = sessions_.begin(); it != sessions_.end();) {
      if (it->second->isClosed()) {
        it = sessions_.erase(it);
      } else {
        ++it;
      }
    }
  }

  RelayGatewayConfig config_;
  mutable std::mutex mutex_;
  std::shared_ptr<journal::JournalReader> reader_;                          // guarded by mutex_
  std::map<brpc::StreamId, std::unique_ptr<RelaySession>> sessions_;        // guarded by mutex_
  std::thread maintenanceThread_;
  std::atomic<bool> stopRequested_{false};
  bool started_ = false;
};

}  // namespace sequencer::gateway::relay::detail
