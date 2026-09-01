#include "counter_fix_codecs.hpp"

#include <hffix.hpp>
#include <hffix_fields.hpp>

#include <cstdlib>
#include <cstring>
#include <string>

namespace sequencer::examples::counter {
namespace {

// Reads one field out of a complete FIX message without allocating.
std::optional<std::string_view> field(const hffix::message_reader& message, int tag) {
  for (auto it = message.begin(); it != message.end(); ++it) {
    if (it->tag() == tag) {
      return std::string_view(it->value().begin(), it->value().size());
    }
  }
  return std::nullopt;
}

}  // namespace

sequencer::Result<sequencer::Bytes> CounterFixInputCodec::toInput(
    const sequencer::ClientRequest& request) {
  const std::string_view raw(reinterpret_cast<const char*>(request.body.data()),
                              request.body.size());
  hffix::message_reader reader(raw.data(), raw.size());
  if (!reader.is_complete() || !reader.is_valid()) {
    return sequencer::Result<sequencer::Bytes>::Error("not a well-formed FIX message");
  }

  const auto type = reader.message_type();
  if (type == reader.end() ||
      std::string_view(type->value().begin(), type->value().size()) != "U1") {
    // A rejection is routine, not exceptional: a FIX session carries
    // plenty this application has no meaning for.
    return sequencer::Result<sequencer::Bytes>::Error("expected a U1 message");
  }

  const std::optional<std::string_view> value = field(reader, kCounterValueTag);
  if (!value.has_value() || value->empty()) {
    return sequencer::Result<sequencer::Bytes>::Error(
        "U1 is missing tag " + std::to_string(kCounterValueTag));
  }

  const std::int64_t delta = std::strtoll(std::string(*value).c_str(), nullptr, 10);
  sequencer::Bytes input(sizeof(delta));
  std::memcpy(input.data(), &delta, sizeof(delta));
  return sequencer::Result<sequencer::Bytes>::Ok(std::move(input));
}

sequencer::Bytes CounterFixInputCodec::toOutput(
    const sequencer::Receipt& receipt, std::span<const sequencer::Payload> designatedOutputs,
    sequencer::Payload input) {
  // --inline_designated_outputs. The chassis withholds designated
  // outputs unless that flag is on, so an empty span here IS the
  // default §8.11 path and must stay silent.
  if (designatedOutputs.empty() || designatedOutputs[0].size() != sizeof(std::int64_t) ||
      input.size() != sizeof(std::int64_t)) {
    return toOutput(receipt, designatedOutputs);
  }
  std::int64_t total = 0;
  std::memcpy(&total, designatedOutputs[0].data(), sizeof(total));
  std::int64_t submitted = 0;
  std::memcpy(&submitted, input.data(), sizeof(submitted));

  // Byte-identical to what CounterFixOutputCodec builds for this same
  // record -- see counterTotalsBody.
  const std::string body = counterTotalsBody(submitted, total);
  return sequencer::Bytes(reinterpret_cast<const std::byte*>(body.data()),
                           reinterpret_cast<const std::byte*>(body.data()) + body.size());
}

sequencer::Bytes CounterFixInputCodec::toOutput(
    const sequencer::Receipt& /*receipt*/,
    std::span<const sequencer::Payload> /*designatedOutputs*/) {
  // Nothing goes back on the propose path for a session transport
  // (§8.11); the transport discards whatever this returns. Returning
  // empty rather than building a message makes that explicit.
  return sequencer::Bytes();
}

std::optional<sequencer::Bytes> CounterFixInputCodec::onDisconnect(
    const sequencer::SessionInfo& /*session*/) {
  // The counter has no notion of a disconnect input -- there is no
  // per-session state to unwind (§8.1).
  return std::nullopt;
}

void CounterFixOutputCodec::toOutput(const sequencer::journal::RecordView& record,
                                      sequencer::Fanout& fanout) {
  for (std::size_t i = 0; i < record.outputCount(); ++i) {
    const sequencer::Payload output = record.output(i);
    if (output.size() != sizeof(std::int64_t)) {
      continue;
    }
    std::int64_t total = 0;
    std::memcpy(&total, output.data(), sizeof(total));

    // Echo the SUBMITTED DELTA back alongside the total, from
    // record.input() -- the journal stores the input beside the outputs,
    // so this is reading a field that was always there.
    //
    // It exists for correlation, and the reasoning is worth keeping
    // because it looks like a hack until you see what it replaces. A
    // FIX client needs to match a reply to the order that caused it.
    // The natural mechanisms cannot work here: Fanout::toSession needs
    // the owning session id, and a per-session topic needs the same
    // thing, and NEITHER is in the record -- the counter's input is
    // eight bytes of delta and nothing else. Carrying a session id or a
    // nonce BESIDE the delta would widen the input to sixteen bytes,
    // which CounterStateMachine::apply rejects outright, so it would
    // mean changing the state machine and leaving journals that older
    // code cannot replay.
    //
    // Echoing the delta avoids all of that: the input encoding, the
    // state machine and §11 replay are untouched. A benchmark client
    // then sends a DISTINCT delta per request and matches on it (see
    // bench/load_generator's FIX arm), which is what makes a
    // multi-client measurement valid -- with every client sharing this
    // broadcast topic, matching replies by arrival order silently
    // completes each client's requests against other clients' totals.
    //
    // The cost is honest and bounded: a benchmark's running total
    // becomes a meaningless sum of nonces. Nobody reads the counter's
    // value, so that is acceptable HERE and is not a pattern to copy. A
    // real order-entry application carries a ClOrdID and addresses
    // toSession.
    std::int64_t submitted = 0;
    const sequencer::Payload in = record.input();
    if (in.size() == sizeof(submitted)) {
      std::memcpy(&submitted, in.data(), sizeof(submitted));
    }

    // The body only: FixOutputTransport adds MsgSeqNum, SendingTime and
    // CheckSum through the session core, so a codec never touches
    // sequence numbers.
    const std::string body = counterTotalsBody(submitted, total);
    // The submitting client's own topic, recovered from the delta's high
    // bits, so only that client receives this -- see kClientIdShift for
    // why a single shared topic could not carry a multi-client run.
    const std::int64_t clientId = counterClientIdOf(submitted);
    fanout.broadcast(counterTopicFor(clientId),
                      sequencer::Bytes(reinterpret_cast<const std::byte*>(body.data()),
                                        reinterpret_cast<const std::byte*>(body.data()) +
                                            body.size()));
  }
}

}  // namespace sequencer::examples::counter
