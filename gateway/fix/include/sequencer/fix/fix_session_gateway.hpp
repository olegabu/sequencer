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

  // Answer each order from the propose receipt instead of waiting for
  // the output half to deliver the same execution report from the
  // journal. Saves that hop (~200us measured) at the cost of the
  // property that makes the journal a resend store: see
  // InputGatewayConfig::inlineDesignatedOnSession for exactly what is
  // traded and when it is sound. Default off = specification.md §8.11.
  bool inlineDesignatedOutputs = false;
};

// Runs until stopped. Blocks.
int RunFixSessionGateway(SessionGatewayConfig config,
                          std::unique_ptr<sequencer::InputCodec> inputCodec,
                          std::unique_ptr<sequencer::OutputCodec> outputCodec);

struct MarketDataGatewayConfig {
  int listenPort = 0;
  std::filesystem::path dataDir;
  std::filesystem::path resumeFile;
  std::string senderCompId = "SEQUENCER-MD";
  int heartBtInt = 30;
  std::filesystem::path sequenceStoreDir;
};

// The output-only shape (specification.md §8.12 "Shape"): a FIX gateway
// that only ever sends, fed by Fanout::broadcast, with subscription by
// the client's own MarketDataRequest.
//
// Note what it does NOT take: no node endpoints and no InputCodec. That
// is a structural guarantee rather than a convention -- there is no
// proposer anywhere in this configuration, so a market-data gateway
// cannot submit to the raft group even if a client sends it something
// that looks like an order. It can therefore be deployed where an
// order-entry gateway should not be, and a compromise of it can submit
// nothing.
//
// Recovery works exactly as it does for order entry: a market-data
// session is a FIX session, so it drops, reconnects, and is caught up
// from the journal and served ResendRequests from it. See
// gateway/fix/README.md's "Recovery: three mechanisms".
int RunFixMarketDataGateway(MarketDataGatewayConfig config,
                             std::unique_ptr<sequencer::OutputCodec> outputCodec);

}  // namespace sequencer::fix
