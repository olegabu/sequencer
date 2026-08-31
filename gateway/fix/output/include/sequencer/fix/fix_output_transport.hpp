#pragma once

// FixOutputTransport — FIX 4.4 delivery as an OutputTransport
// (specification.md §8.10, §8.12 "Shape"), alongside the WebSocket and
// gRPC transports.
//
// The OutputCodec builds a FIX message BODY -- the application fields
// and nothing else -- and this transport adds the session-layer fields
// (MsgSeqNum, SendingTime, CheckSum) through the session core before
// sending. So a codec never learns about sequence numbers, and the
// session layer never learns about execution reports.
//
// TWO DEPLOYMENT SHAPES, and the difference matters (§8.12 "Shape"):
//
//   Order entry -- this transport shares a session core instance with
//     FixInputTransport, so a client's execution reports arrive on the
//     very session that submitted the order. FIX's convention is that
//     all reports for a session's orders, aggressive and passive, come
//     back on the order-entry session that owns them. Pass the input
//     transport as the session source.
//
//   Market data -- output only, with its own acceptor and no input
//     side. Subscription is FIX's own MarketDataRequest, which resolves
//     §8.10's open topic question the standard way: a session that has
//     requested a symbol receives that topic's broadcasts, and one that
//     has not receives nothing.
//
// specification.md §8.11: a gateway delivers each output to a given
// client exactly once, by the path its transport shape dictates, and
// never by both. This is the ONLY path a FIX client's outputs take --
// the input side deliberately answers an order with nothing.

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include <sequencer/fix/fix_session.hpp>
#include <sequencer/fix/session_source.hpp>
#include <sequencer/output_transport.hpp>

namespace sequencer::fix {

// What the transport remembers so a ResendRequest can be served from
// the journal rather than from a message store (§8.12 reason 1).
struct SentRecord {
  std::uint64_t journalSequenceNumber = 0;
  std::uint32_t outputIndex = 0;
  std::string msgType;
  std::string sendingTime;
};

class FixOutputTransport : public sequencer::OutputTransport {
 public:
  // Order-entry shape: shares `sessions` with the input side.
  explicit FixOutputTransport(SessionSource& sessions);
  ~FixOutputTransport() override;

  FixOutputTransport(const FixOutputTransport&) = delete;
  FixOutputTransport& operator=(const FixOutputTransport&) = delete;

  void attach(sequencer::BroadcastRing& ring, sequencer::TopicRegistry& topics,
              int idleSpinIterations) override;
  void start(int listenPort) override;
  void stop() override;

  // The (session, outbound MsgSeqNum) -> journal position mapping that
  // ResendRequest handling needs. Kept in memory per live session and
  // reconstructed on restart by re-reading the journal from the
  // session's last persisted position -- there is no message store.
  const SentRecord* sentRecord(std::uint64_t sessionId, std::uint64_t outboundSeqNum) const;
  std::size_t sentRecordCount(std::uint64_t sessionId) const;

  struct Impl;

 private:
  std::unique_ptr<Impl> impl_;
};

}  // namespace sequencer::fix
