#pragma once

// Ties together the tailing thread, the resume position, the transport,
// and the codec — everything RunOutputGateway (output_gateway.hpp) sets
// up, minus argv/gflags parsing, exactly as node/'s NodeImpl is
// separated from RunNode.

#include <atomic>
#include <chrono>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>

#include <sequencer/journal/reader.hpp>
#include <sequencer/output_codec.hpp>
#include <sequencer/output_transport.hpp>

#include "resume_position.hpp"

namespace sequencer::gateway::output::detail {

struct OutputGatewayConfig {
  std::filesystem::path dataDir;     // a node's journal directory, colocated (§3)
  std::filesystem::path resumeFile;  // durable resume position
  int listenPort = 0;
};

class OutputGatewayImpl {
 public:
  OutputGatewayImpl(OutputGatewayConfig config, std::unique_ptr<sequencer::OutputCodec> codec,
                     std::unique_ptr<sequencer::OutputTransport> transport)
      : config_(std::move(config)),
        codec_(std::move(codec)),
        transport_(std::move(transport)),
        resumePosition_(config_.resumeFile) {}

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
    transport_->start(config_.listenPort);
    tailThread_ = std::thread([this] { tailLoop(); });
    started_ = true;
  }

  void stop() {
    stopRequested_.store(true, std::memory_order_relaxed);
    if (tailThread_.joinable()) {
      tailThread_.join();
    }
    transport_->stop();
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
      codec_->toOutput(reader->record(seq), *transport_);
      ++seq;
      resumePosition_.store(seq);
    }
  }

  OutputGatewayConfig config_;
  std::unique_ptr<sequencer::OutputCodec> codec_;
  std::unique_ptr<sequencer::OutputTransport> transport_;
  ResumePosition resumePosition_;
  std::thread tailThread_;
  std::atomic<bool> stopRequested_{false};
  bool started_ = false;
};

}  // namespace sequencer::gateway::output::detail
