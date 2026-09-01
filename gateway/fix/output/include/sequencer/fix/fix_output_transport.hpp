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
#include <filesystem>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include <sequencer/fix/fix_session.hpp>
#include <sequencer/fix/session_source.hpp>
#include <sequencer/output_codec.hpp>
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

  // Serves FIX ResendRequests from the JOURNAL, which is
  // specification.md §8.12's reason 1 made real: there is no outbound
  // message store, so a resend re-reads the record that produced the
  // message and re-runs the codec over it. Deterministic by
  // construction -- the same record through the same codec is the same
  // bytes -- which is why no second copy is needed.
  //
  // `codec` must outlive this transport; the session gateway owns it.
  void attachJournal(const std::filesystem::path& dataDir, sequencer::OutputCodec& codec);

  // Sends one output on a session from the INPUT half's propose receipt
  // rather than from the ring (InputGatewayConfig::
  // inlineDesignatedOnSession). Routed through the same deliver() the
  // ring reader uses, so it takes the same MsgSeqNum assignment, the
  // same (journal record, output index) high-water mark, and the same
  // recordSent() bookkeeping a resend needs. Marking the high-water
  // mark here is what makes the journal copy of this output a no-op
  // when it arrives a couple of hundred microseconds later.
  void deliverInline(std::uint64_t sessionId, std::string_view body,
                     std::uint64_t journalSequenceNumber, std::uint32_t lastOutputIndex);

  // Sends a session everything addressed to it since its last persisted
  // journal position, as NEW messages. Called automatically when a
  // session completes Logon; exposed for tests.
  //
  // This is NOT a resend, and the distinction matters: an output
  // addressed to a disconnected session was never sent, so no outbound
  // sequence number exists to replay. ResendRequest recovers messages
  // the gateway DID send; catch-up delivers the ones it could not.
  void catchUp(FixSession& session);

  // The (FIX session, outbound MsgSeqNum) -> journal position mapping
  // that ResendRequest handling needs. There is no message store: a
  // resend re-reads the record named here.
  //
  // Keyed by SESSION KEY -- the CompID pair -- not by the connection's
  // routing id. That distinction is the whole point: a routing id is
  // new on every reconnect, so positions recorded before a drop would
  // be invisible to the session that comes back, and a resend would
  // degrade to a gap fill exactly when it is most needed. The CompID
  // pair survives the socket, as the sequence counters keyed by it do.
  const SentRecord* sentRecord(const std::string& sessionKey,
                                std::uint64_t outboundSeqNum) const;
  std::size_t sentRecordCount(const std::string& sessionKey) const;

  struct Impl;

 private:
  std::unique_ptr<Impl> impl_;
};

}  // namespace sequencer::fix
