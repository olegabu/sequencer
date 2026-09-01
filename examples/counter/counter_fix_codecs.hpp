#pragma once

// The counter example over FIX (specification.md §10, §8.12).
//
// Two user-defined message types, in FIX's private range:
//
//   U1  submit a delta   -- tag 5001 carries the signed integer
//   U2  the new total    -- tag 5001 carries it back
//
// WHY U2 IS BROADCAST RATHER THAN ADDRESSED TO THE SUBMITTER, which is
// the interesting part of this file.
//
// specification.md §8.11 says a session transport delivers a client's
// outputs from the journal. To address the submitting session, the
// OutputCodec must be able to tell which session that was -- and all it
// receives is the journal record. The counter's input is exactly eight
// bytes, a delta and nothing else (CounterStateMachine::apply rejects
// anything else outright), so the record simply does not contain a
// session id and no codec can invent one.
//
// Carrying it would mean widening the counter's input encoding to
// delta + session id, which changes the bytes in the journal and so
// changes what §11's determinism replay reproduces. That is a real
// decision about an application's wire format, not a gateway detail,
// and this example does not make it -- ClientRequest::sessionId now
// carries the value to any application that wants to.
//
// So the counter broadcasts on the "totals" topic, exactly as its
// WebSocket and gRPC output paths already do, and a FIX client
// subscribes with a MarketDataRequest (35=V, tag 55=TOTALS). That is
// the standard FIX subscription mechanism and is what §8.10's topic
// question resolves to.
//
// What this example DOES demonstrate, and what it exists to prove: the
// total reaches the client from the JOURNAL, never as the synchronous
// reply to the order, even though CounterStateMachine designates it.
// That is §8.11's exactly-once rule, and counter_fix_gateway_test
// asserts it.

#include <cstdint>
#include <optional>
#include <string>
#include <span>

#include <sequencer/input_codec.hpp>
#include <sequencer/output_codec.hpp>

namespace sequencer::examples::counter {

// The private tag both directions use for the counter's value.
inline constexpr int kCounterValueTag = 5001;

// U2 echoes the delta that produced it, in this tag, so a client can
// match a reply to its own order. See CounterFixOutputCodec::toOutput
// for why echoing the input is the only correlation available here and
// what it costs.
inline constexpr int kCounterEchoTag = 5000;

// U2 is published on a PER-CLIENT topic, "TOTALS-<id>", and a client
// subscribes only to its own by naming it as a Symbol (tag 55) in a
// MarketDataRequest.
//
// A single shared topic is what the obvious implementation does, and it
// does not scale: every subscriber receives every output, so a
// gateway's delivery load is (rate x subscribers) rather than rate. On
// a five-client fleet run at 100k that meant each gateway delivering
// ~500k messages/sec for 100k of offered load, and the gateway folded
// -- while the same configuration from ONE client sustained 20k at
// 699us. The fan-out was the limit, not the gateway.
//
// The client id is carried in the HIGH BITS of the submitted delta:
//
//     delta = (clientId << kClientIdShift) | sequence
//
// which costs nothing, because the delta is an opaque int64 the state
// machine only sums. The output codec recovers the id by shifting, and
// publishes to that client's own topic, so fan-out is exactly one.
//
// This is a benchmark technique, not a pattern to copy. A real
// application addresses Fanout::toSession with the owning session --
// see this file's toOutput() for why the counter cannot.
inline constexpr char kTotalsTopicPrefix[] = "TOTALS-";
inline constexpr int kClientIdShift = 40;

// The topic a given client id subscribes to and receives on.
inline std::string counterTopicFor(std::int64_t clientId) {
  return std::string(kTotalsTopicPrefix) + std::to_string(clientId);
}

// The client id encoded in a submitted delta, or 0 if it carries none.
//
// A delta that is NEGATIVE or too small to hold an id belongs to client
// 0. That covers every ordinary use of the counter -- a person sending
// -2 through the demo is not encoding anything -- and it has to be
// stated because the arithmetic bites otherwise: -2 >> 40 is -1, not 0,
// so a negative delta published to "TOTALS--1", a topic nobody
// subscribes to, and its total simply vanished. The end-to-end test
// caught it because it submits 5 then -2.
// The U2 body both delivery paths produce.
//
// Shared deliberately: with --inline_designated_outputs the input codec
// answers from the propose receipt while a ResendRequest re-runs the
// OUTPUT codec over the journal record. If those two built the message
// differently, a resend would replay something the client never
// received. One function means they cannot drift.
inline std::string counterTotalsBody(std::int64_t submitted, std::int64_t total) {
  return "35=U2\001" + std::to_string(kCounterEchoTag) + "=" + std::to_string(submitted) +
         "\001" + std::to_string(kCounterValueTag) + "=" + std::to_string(total) + "\001";
}

inline std::int64_t counterClientIdOf(std::int64_t delta) {
  return delta > 0 ? (delta >> kClientIdShift) : 0;
}

// U1 -> the 8-byte delta CounterStateMachine expects.
class CounterFixInputCodec : public sequencer::InputCodec {
 public:
  sequencer::Result<sequencer::Bytes> toInput(const sequencer::ClientRequest& request) override;

  // On a session transport the chassis hands this an EMPTY span
  // whatever the state machine designated (§8.11), so there is nothing
  // to encode and nothing is sent -- FixInputTransport discards the
  // result. It is implemented rather than left to throw because the
  // same codec is usable behind a request/response transport, where
  // the designated total IS the reply.
  // Builds the U2 reply from the designated total plus the submitted
  // delta, for --inline_designated_outputs. Without that flag the
  // chassis passes no designated outputs and this falls through to the
  // silent two-argument form below.
  sequencer::Bytes toOutput(const sequencer::Receipt& receipt,
                             std::span<const sequencer::Payload> designatedOutputs,
                             sequencer::Payload input) override;

  sequencer::Bytes toOutput(const sequencer::Receipt& receipt,
                             std::span<const sequencer::Payload> designatedOutputs) override;

  std::optional<sequencer::Bytes> onDisconnect(const sequencer::SessionInfo& session) override;
};

// A journal record -> U2 broadcast on the totals topic.
class CounterFixOutputCodec : public sequencer::OutputCodec {
 public:
  void toOutput(const sequencer::journal::RecordView& record, sequencer::Fanout& fanout) override;
};

}  // namespace sequencer::examples::counter
