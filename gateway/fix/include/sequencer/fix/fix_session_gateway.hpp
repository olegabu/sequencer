#pragma once

// RunFixSessionGateway — the order-entry FIX gateway: one process, one
// listening port, and ONE session core shared between its input and
// output halves (specification.md §8.12 "Shape").
//
// That sharing is the whole point. FIX's convention is that every
// execution report for a session's orders -- the aggressive fill on the
// order you just sent, and the passive fill on the resting order you
// sent an hour ago -- arrives on the order-entry session that owns
// them. Two independent gateways could not do that: the output side
// would have no way to reach the socket the order came in on.
//
// Delivery follows §8.11 exactly, and this is where it becomes visible
// end to end:
//
//   - an order arrives over FIX and is proposed to the raft group;
//   - the synchronous receipt is consumed HERE, for admission
//     confirmation and sequence-number bookkeeping, and never becomes
//     a FIX message;
//   - the execution report reaches the client from the JOURNAL, via
//     the output half, in sequence-number order.
//
// So each output reaches a session exactly once, and a client's fills
// across different orders arrive in journal order -- which position and
// risk logic downstream depend on.

#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include <sequencer/input_codec.hpp>
#include <sequencer/output_codec.hpp>

namespace sequencer::fix {

struct SessionGatewayConfig {
  // The raft group's Propose endpoints, for the input half.
  std::vector<std::string> nodeEndpoints;
  // The FIX port clients connect to. One port: order entry and
  // execution reports share the session, so they share the socket.
  int listenPort = 0;

  // The journal this gateway tails, colocated (§3), and where its
  // resume position is kept.
  std::filesystem::path dataDir;
  std::filesystem::path resumeFile;

  // This gateway's own CompID. Client identity is adopted from each
  // Logon's SenderCompID, so it is not configured here.
  std::string senderCompId = "SEQUENCER";
  int heartBtInt = 30;
  // Where the per-session sequence counters are persisted -- the only
  // session state that must outlive the process (§8.12).
  std::filesystem::path sequenceStoreDir;
};

// Runs until stopped. Blocks.
int RunFixSessionGateway(SessionGatewayConfig config,
                          std::unique_ptr<sequencer::InputCodec> inputCodec,
                          std::unique_ptr<sequencer::OutputCodec> outputCodec);

}  // namespace sequencer::fix
