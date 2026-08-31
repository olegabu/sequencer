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
#include <span>

#include <sequencer/input_codec.hpp>
#include <sequencer/output_codec.hpp>

namespace sequencer::examples::counter {

// The private tag both directions use for the counter's value.
inline constexpr int kCounterValueTag = 5001;

// The topic U2 is broadcast on; a client subscribes by naming it as a
// Symbol (tag 55) in a MarketDataRequest.
inline constexpr char kTotalsTopic[] = "TOTALS";

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
