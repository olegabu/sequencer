#pragma once

// The order-entry FIX gateway on QuickFIX's session layer
// (specification.md §8.13). The counterpart to
// RunFixSessionGateway, taking the same codecs and serving the same
// journal; only the session layer differs.

#include <sequencer/input_codec.hpp>
#include <sequencer/output_codec.hpp>

#include <filesystem>
#include <memory>
#include <string>
#include <vector>

namespace sequencer::quickfix {

struct QuickFixGatewayConfig {
  std::vector<std::string> nodeEndpoints;
  int listenPort = 0;
  std::filesystem::path dataDir;
  std::filesystem::path resumeFile;
  std::string senderCompId = "SEQUENCER";
  int heartBtInt = 30;
  std::filesystem::path sequenceStoreDir;
  // Required, unlike the hffix gateway's: QuickFIX 1.15.1 declares its
  // acceptor sessions up front and has no dynamic ones, so every
  // counterparty is named here before it connects.
  std::vector<std::string> clientCompIds;
};

int RunQuickFixSessionGateway(QuickFixGatewayConfig config,
                              std::unique_ptr<sequencer::InputCodec> inputCodec,
                              std::unique_ptr<sequencer::OutputCodec> outputCodec);

}  // namespace sequencer::quickfix
