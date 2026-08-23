#pragma once

// Deliberately minimal JSON helpers, shared by CounterInputCodec and
// CounterOutputCodec — see counter_input_codec.hpp for why this isn't a
// real JSON library. Every shape this example produces or consumes is a
// flat object of integer fields; nothing here needs to handle more than
// that.

#include <charconv>
#include <cstdint>
#include <optional>
#include <string>

namespace sequencer::examples::counter {

// Finds "fieldName": <integer> within `json` and parses the integer.
// Tolerant of surrounding whitespace around the colon; intolerant of
// anything else about the object's shape — no nested objects, no
// strings, no other fields need parsing for this example.
inline std::optional<std::int64_t> extractJsonIntField(const std::string& json,
                                                         const std::string& fieldName) {
  const std::string key = "\"" + fieldName + "\"";
  const std::size_t keyPos = json.find(key);
  if (keyPos == std::string::npos) {
    return std::nullopt;
  }
  std::size_t pos = keyPos + key.size();
  while (pos < json.size() && (json[pos] == ' ' || json[pos] == '\t')) {
    ++pos;
  }
  if (pos >= json.size() || json[pos] != ':') {
    return std::nullopt;
  }
  ++pos;
  while (pos < json.size() && (json[pos] == ' ' || json[pos] == '\t')) {
    ++pos;
  }

  const std::size_t start = pos;
  if (pos < json.size() && json[pos] == '-') {
    ++pos;
  }
  const std::size_t digitsStart = pos;
  while (pos < json.size() && json[pos] >= '0' && json[pos] <= '9') {
    ++pos;
  }
  if (pos == digitsStart) {
    return std::nullopt;  // no digits found
  }

  std::int64_t value = 0;
  const auto result = std::from_chars(json.data() + start, json.data() + pos, value);
  if (result.ec != std::errc() || result.ptr != json.data() + pos) {
    return std::nullopt;
  }
  return value;
}

// Builds {"sequence_number":<N>,"total":<M>} — the one object shape
// both CounterInputCodec::toOutput and CounterOutputCodec::toOutput
// produce.
inline std::string buildSequenceAndTotalJson(std::uint64_t sequenceNumber, std::int64_t total) {
  return "{\"sequence_number\":" + std::to_string(sequenceNumber) + ",\"total\":" +
         std::to_string(total) + "}";
}

}  // namespace sequencer::examples::counter
