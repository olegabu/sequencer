#include <sequencer/quickfix/quickfix_session_gateway.hpp>

#include <sequencer/journal/journal.hpp>
#include <sequencer/quickfix/journal_message_store.hpp>
#include <sequencer/quickfix/quickfix_input_transport.hpp>
#include <sequencer/quickfix/quickfix_output_transport.hpp>

#include "../../input/src/input_gateway_impl.hpp"
#include "../../output/src/output_gateway_impl.hpp"

#include <atomic>
#include <csignal>
#include <fstream>
#include <map>
#include <mutex>
#include <thread>

namespace sequencer::quickfix {
namespace {

std::atomic<bool> gStopRequested{false};
void handleStopSignal(int) { gStopRequested.store(true, std::memory_order_relaxed); }

// The BodySource the message store rebuilds from: re-read the journal
// record, re-run the OUTPUT codec, hand back the bytes it produced.
// Deterministic because the codec is a pure function of the record --
// which is the property that lets a resend be served without a copy.
class JournalBodySource : public BodySource {
 public:
  JournalBodySource(const std::filesystem::path& dataDir, sequencer::OutputCodec& codec)
      : codec_(codec) {
    try {
      journal_ = std::make_unique<sequencer::journal::JournalReader>(dataDir / "journal");
    } catch (const std::exception&) {
      journal_.reset();
    }
  }

  bool bodyFor(std::uint64_t journalSequenceNumber, std::uint32_t outputIndex,
                std::string& msgTypeOut, std::string& bodyOut) override {
    std::lock_guard<std::mutex> lock(mutex_);
    if (journal_ == nullptr || !journal_->contains(journalSequenceNumber)) {
      return false;
    }
    Captured captured;
    codec_.toOutput(journal_->record(journalSequenceNumber), captured);
    if (outputIndex >= captured.outputs.size()) {
      return false;
    }
    const sequencer::Bytes& raw = captured.outputs[outputIndex];
    std::string body(reinterpret_cast<const char*>(raw.data()), raw.size());
    // Split the codec's leading MsgType, as every delivery path does.
    msgTypeOut = "U2";
    if (body.size() > 3 && body.compare(0, 3, "35=") == 0) {
      const std::size_t soh = body.find('\001');
      if (soh != std::string::npos) {
        msgTypeOut = body.substr(3, soh - 3);
        body = body.substr(soh + 1);
      }
    }
    bodyOut = std::move(body);
    return true;
  }

 private:
  // Collects what the codec emits for one record, in emission order, so
  // an output index can be resolved back to bytes.
  struct Captured : public sequencer::Fanout {
    void toSession(std::uint64_t, sequencer::Bytes bytes) override {
      outputs.push_back(std::move(bytes));
    }
    void broadcast(const std::string&, sequencer::Bytes bytes) override {
      outputs.push_back(std::move(bytes));
    }
    std::vector<sequencer::Bytes> outputs;
  };

  sequencer::OutputCodec& codec_;
  std::unique_ptr<sequencer::journal::JournalReader> journal_;
  std::mutex mutex_;
};

// The sequence-number pair, persisted per session. The same job
// gateway/fix/'s FileSequenceStore does, against QuickFIX's interface.
class FileSequences : public SequenceNumberStore {
 public:
  explicit FileSequences(std::filesystem::path directory) : directory_(std::move(directory)) {
    if (!directory_.empty()) {
      std::filesystem::create_directories(directory_);
    }
  }

  void load(const std::string& sessionKey, int& nextSender, int& nextTarget) override {
    std::lock_guard<std::mutex> lock(mutex_);
    nextSender = 1;
    nextTarget = 1;
    if (directory_.empty()) {
      const auto it = memory_.find(sessionKey);
      if (it != memory_.end()) {
        nextSender = it->second.first;
        nextTarget = it->second.second;
      }
      return;
    }
    std::ifstream in(pathFor(sessionKey));
    if (in) {
      in >> nextSender >> nextTarget;
      if (!in) {
        nextSender = 1;
        nextTarget = 1;
      }
    }
  }

  void save(const std::string& sessionKey, int nextSender, int nextTarget) override {
    std::lock_guard<std::mutex> lock(mutex_);
    if (directory_.empty()) {
      memory_[sessionKey] = {nextSender, nextTarget};
      return;
    }
    // Never throws: a store that takes the gateway down on a full disk
    // would be worse than one that loses a counter, and gateway/fix/
    // learned that the hard way.
    try {
      const std::filesystem::path tmp = pathFor(sessionKey) + ".tmp";
      {
        std::ofstream out(tmp, std::ios::trunc);
        out << nextSender << " " << nextTarget << "\n";
      }
      std::filesystem::rename(tmp, pathFor(sessionKey));
    } catch (const std::exception&) {
    }
  }

 private:
  std::string pathFor(const std::string& sessionKey) const {
    std::string safe;
    for (const char c : sessionKey) {
      safe += (std::isalnum(static_cast<unsigned char>(c)) != 0) ? c : '_';
    }
    return (directory_ / safe).string();
  }

  std::filesystem::path directory_;
  std::mutex mutex_;
  std::map<std::string, std::pair<int, int>> memory_;
};

}  // namespace

int RunQuickFixSessionGateway(QuickFixGatewayConfig config,
                              std::unique_ptr<sequencer::InputCodec> inputCodec,
                              std::unique_ptr<sequencer::OutputCodec> outputCodec) {
  gStopRequested.store(false, std::memory_order_relaxed);

  QuickFixInputConfig inputConfig;
  inputConfig.senderCompId = config.senderCompId;
  inputConfig.heartBtInt = config.heartBtInt;
  inputConfig.clientCompIds = config.clientCompIds;
  inputConfig.sequenceStoreDir = config.sequenceStoreDir.string();

  auto ownedInput = std::make_shared<std::unique_ptr<QuickFixInputTransport>>(
      std::make_unique<QuickFixInputTransport>(inputConfig));
  QuickFixInputTransport* input = ownedInput->get();

  sequencer::OutputCodec* codecForResends = outputCodec.get();
  FileSequences sequences(config.sequenceStoreDir);
  JournalBodySource bodies(config.dataDir, *codecForResends);
  JournalMessageStoreFactory storeFactory(bodies, sequences);
  input->setStoreFactory(&storeFactory);

  sequencer::gateway::output::detail::OutputGatewayConfig outputConfig;
  outputConfig.dataDir = config.dataDir;
  outputConfig.resumeFile = config.resumeFile;
  auto ownedOutput = std::make_unique<QuickFixOutputTransport>(*input);
  sequencer::gateway::output::detail::OutputGatewayImpl outputGateway(
      outputConfig, std::move(outputCodec),
      std::unique_ptr<sequencer::OutputTransport>(std::move(ownedOutput)), 0);

  sequencer::gateway::input::detail::InputGatewayConfig inputGatewayConfig;
  inputGatewayConfig.nodeEndpoints = config.nodeEndpoints;
  inputGatewayConfig.listenPort = config.listenPort;
  sequencer::gateway::input::detail::InputGatewayImpl inputGateway(
      inputGatewayConfig, std::move(inputCodec), sequencer::acceptAllSignatures,
      [ownedInput]() {
        return std::unique_ptr<sequencer::InputTransport>(std::move(*ownedInput));
      });

  outputGateway.start();
  inputGateway.start();

  std::signal(SIGINT, handleStopSignal);
  std::signal(SIGTERM, handleStopSignal);
  while (!gStopRequested.load(std::memory_order_relaxed)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }

  inputGateway.stop();
  outputGateway.stop();
  return 0;
}

}  // namespace sequencer::quickfix
