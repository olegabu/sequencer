#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

namespace sequencer {

// Pointer + length, zero-copy. See specification.md §4: the state
// machine's apply() and the journal's records are both expressed in
// terms of this type. Defined here, in journal/ (the component with no
// dependencies of its own — specification.md §9), so every other
// component can share one definition without a dependency cycle.
using Payload = std::span<const std::byte>;

// The owning counterpart of Payload — specification.md §8.5's
// InputCodec produces new byte sequences (e.g. JSON delta -> 8-byte
// input) that must outlive the codec call that created them, unlike
// Payload's borrowed view. Lives alongside Payload for the same
// zero-dependency reason.
using Bytes = std::vector<std::byte>;

// specification.md §5.2: "committed at this position; will be
// deterministically processed" — everything a Propose caller learns
// about *where* an input landed. The designated output (if any) rides
// alongside a Receipt wherever one is produced, never inside it (see
// §5.2 for why the two are kept separate).
struct Receipt {
  std::uint64_t sequenceNumber;
};

}  // namespace sequencer
