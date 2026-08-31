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

    // The body only: FixOutputTransport adds MsgSeqNum, SendingTime and
    // CheckSum through the session core, so a codec never touches
    // sequence numbers.
    const std::string body =
        "35=U2\001" + std::to_string(kCounterValueTag) + "=" + std::to_string(total) + "\001";
    fanout.broadcast(kTotalsTopic,
                      sequencer::Bytes(reinterpret_cast<const std::byte*>(body.data()),
                                        reinterpret_cast<const std::byte*>(body.data()) +
                                            body.size()));
  }
}

}  // namespace sequencer::examples::counter
