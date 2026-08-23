#pragma once

// Ties together the tailing thread, the resume position, the client-
// facing brpc server, and the codec — everything RunOutputGateway
// (output_gateway.hpp) sets up, minus argv/gflags parsing, exactly as
// node/'s NodeImpl is separated from RunNode.

#include <brpc/server.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>

#include <sequencer/journal/reader.hpp>
#include <sequencer/output_codec.hpp>

#include "output_subscribe_service_impl.hpp"
#include "resume_position.hpp"
#include "stream_fanout.hpp"

namespace sequencer::gateway::output::detail {

struct OutputGatewayConfig {
  std::filesystem::path dataDir;     // a node's journal directory, colocated (§3)
  std::filesystem::path resumeFile;  // durable resume position
  int listenPort = 0;
};

class OutputGatewayImpl {
 public:
  OutputGatewayImpl(OutputGatewayConfig config, std::unique_ptr<sequencer::OutputCodec> codec)
      : config_(std::move(config)),
        codec_(std::move(codec)),
        resumePosition_(config_.resumeFile),
        subscribeService_(fanout_) {}

  OutputGatewayImpl(const OutputGatewayImpl&) = delete;
  OutputGatewayImpl& operator=(const OutputGatewayImpl&) = delete;

  // A still-joinable std::thread member calls std::terminate() on
  // destruction — matters here because a fatal test assertion (or any
  // exception) can unwind past an explicit stop() call.
  ~OutputGatewayImpl() {
    if (started_) {
      stop();
    }
  }

  void start() {
    if (server_.AddService(&subscribeService_, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
      throw std::runtime_error("OutputGatewayImpl::start: AddService(OutputSubscribeService) failed");
    }
    brpc::ServerOptions serverOptions;
    if (server_.Start(config_.listenPort, &serverOptions) != 0) {
      throw std::runtime_error("OutputGatewayImpl::start: brpc::Server::Start failed on port " +
                                std::to_string(config_.listenPort));
    }
    tailThread_ = std::thread([this] { tailLoop(); });
    started_ = true;
  }

  void stop() {
    stopRequested_.store(true, std::memory_order_relaxed);
    if (tailThread_.joinable()) {
      tailThread_.join();
    }
    // brpc::Server::Join() waits for its acceptor to see every accepted
    // connection fully closed; a Subscribe client that never
    // voluntarily disconnects would otherwise hang shutdown forever.
    fanout_.closeAll();
    server_.Stop(0);
    server_.Join();
    started_ = false;
  }

  int listenPort() const { return config_.listenPort; }

 private:
  void tailLoop() {
    std::uint64_t seq = resumePosition_.load();
    std::unique_ptr<journal::JournalReader> reader;
    while (!stopRequested_.load(std::memory_order_relaxed)) {
      if (!reader) {
        try {
          reader = std::make_unique<journal::JournalReader>(config_.dataDir / "journal.data",
                                                              config_.dataDir / "journal.index");
        } catch (const std::exception&) {
          // The journal file pair may not exist yet — retry.
          std::this_thread::sleep_for(std::chrono::milliseconds(50));
          continue;
        }
      }
      if (!reader->contains(seq)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        continue;
      }
      // specification.md §8.3: "in sequence-number order... in order,
      // exactly once." One codec call per record, strictly in order,
      // with the resume position advanced only after the call returns.
      codec_->toOutput(reader->record(seq), fanout_);
      ++seq;
      resumePosition_.store(seq);
    }
  }

  OutputGatewayConfig config_;
  std::unique_ptr<sequencer::OutputCodec> codec_;
  ResumePosition resumePosition_;
  StreamFanout fanout_;
  OutputSubscribeServiceImpl subscribeService_;
  brpc::Server server_;
  std::thread tailThread_;
  std::atomic<bool> stopRequested_{false};
  bool started_ = false;
};

}  // namespace sequencer::gateway::output::detail
