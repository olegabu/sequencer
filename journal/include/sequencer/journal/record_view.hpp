#pragma once

// RecordView — a lazy, zero-copy accessor over one JournalRecord's bytes
// (specification.md §6.4), plus the free functions that encode a record
// into that same layout. Both live here because they are two views of
// one fact: the on-disk field layout declared in format.hpp.

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>

#include <sequencer/journal/format.hpp>
#include <sequencer/payload.hpp>

namespace sequencer::journal {

using sequencer::Payload;

// Exact byte size append() will write for this input and these outputs
// — specification.md §6.2's field layout, summed.
inline std::size_t recordEncodedSize(Payload input, std::span<const Payload> outputs) noexcept {
  std::size_t total = 8 /* sequenceNumber */ + 4 /* inputLength */ + input.size() +
                       2 /* outputCount */;
  for (const auto& output : outputs) {
    total += 4 /* length */ + output.size();
  }
  return total;
}

// Serializes one record into `dst`, which must point at least
// recordEncodedSize(input, outputs) writable bytes. This is step 1 of
// the write protocol (§6.3) only — the caller (writer.hpp) is
// responsible for step 2 (the index entry) and step 3 (the
// release-store of the committed count) that make the record visible.
inline void encodeRecord(std::byte* dst, std::uint64_t sequenceNumber, Payload input,
                          std::span<const Payload> outputs) noexcept {
  detail::putU64(dst, sequenceNumber);
  dst += 8;
  detail::putU32(dst, static_cast<std::uint32_t>(input.size()));
  dst += 4;
  std::memcpy(dst, input.data(), input.size());
  dst += static_cast<std::ptrdiff_t>(input.size());
  detail::putU16(dst, static_cast<std::uint16_t>(outputs.size()));
  dst += 2;
  for (const auto& output : outputs) {
    detail::putU32(dst, static_cast<std::uint32_t>(output.size()));
    dst += 4;
    std::memcpy(dst, output.data(), output.size());
    dst += static_cast<std::ptrdiff_t>(output.size());
  }
}

// A read-only, zero-copy view over one already-published record's bytes
// — `input()`, `output(i)`, and `rawBytes()` all alias the underlying
// mapping directly, with no copying (§6.4). Valid only as long as the
// owning JournalReader/JournalWriter's mapping remains alive, and only
// for a record already covered by the committed count (§6.3) — this
// type performs no synchronization of its own.
class RecordView {
 public:
  RecordView(const std::byte* base, std::uint32_t length) noexcept : base_(base), length_(length) {}

  std::uint64_t sequenceNumber() const noexcept { return detail::getU64(base_); }

  Payload input() const noexcept {
    const std::uint32_t len = detail::getU32(base_ + 8);
    return Payload(base_ + 12, len);
  }

  std::uint16_t outputCount() const noexcept {
    const std::uint32_t inputLen = detail::getU32(base_ + 8);
    return detail::getU16(base_ + 12 + inputLen);
  }

  // `index` must be < outputCount(). Outputs are stored back-to-back
  // with a length prefix each, so this walks from the start in O(index)
  // — fine for the small output counts real state machines emit; a
  // caller iterating every output should do so once in index order
  // rather than calling output(i) for each i independently, to avoid
  // repeated re-walking of the prefix.
  Payload output(std::uint16_t index) const noexcept {
    const std::uint32_t inputLen = detail::getU32(base_ + 8);
    const std::byte* p = base_ + 12 + inputLen + 2;
    for (std::uint16_t i = 0; i < index; ++i) {
      const std::uint32_t len = detail::getU32(p);
      p += 4 + len;
    }
    const std::uint32_t len = detail::getU32(p);
    return Payload(p + 4, len);
  }

  // The complete record — header and all — exactly as append() wrote
  // it. This is what a leaf hash (specification.md §7.2) is computed
  // over: hash(rawBytes() ++ sequenceNumber).
  Payload rawBytes() const noexcept { return Payload(base_, length_); }

 private:
  const std::byte* base_;
  std::uint32_t length_;
};

}  // namespace sequencer::journal
