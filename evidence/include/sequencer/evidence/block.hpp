#pragma once

// Fixed block-cutting (specification.md §7.1): every kBlockSize
// consecutive sequence numbers form one block, 1-indexed both ways —
// sequence numbers 1..1024 are block 1, 1025..2048 are block 2. This is
// a fixed, deterministic rule, not a runtime setting — "changing the
// block size is itself a sequenced administrative command," which is
// why this is a compile-time constant here rather than a flag.
//
// Depends on nothing (not even a cryptography library) — pure integer
// arithmetic, so anything needing to reason about block boundaries
// (evidence/'s own signing gateway, sdk/'s proof verifier) can include
// just this.

#include <cstdint>
#include <stdexcept>

namespace sequencer::evidence {

inline constexpr std::uint64_t kBlockSize = 1024;

struct BlockBounds {
  std::uint64_t firstSequenceNumber;
  std::uint64_t lastSequenceNumber;
};

// Sequence numbers are 1-based (specification.md §2.1); block indices
// are too, so blockIndexForSequence(1) == 1, matching "sequence numbers
// 1-1024 are block 1."
inline std::uint64_t blockIndexForSequence(std::uint64_t sequenceNumber) {
  if (sequenceNumber == 0) {
    throw std::invalid_argument("blockIndexForSequence: sequence numbers are 1-based");
  }
  return (sequenceNumber - 1) / kBlockSize + 1;
}

inline BlockBounds blockBounds(std::uint64_t blockIndex) {
  if (blockIndex == 0) {
    throw std::invalid_argument("blockBounds: block indices are 1-based");
  }
  return BlockBounds{.firstSequenceNumber = (blockIndex - 1) * kBlockSize + 1,
                      .lastSequenceNumber = blockIndex * kBlockSize};
}

// A block is signable once the journal has committed every sequence
// number through its last — specification.md §7.1 never cuts a partial
// block ("at low input rates a block can take a long time to fill" is
// stated as an accepted latency tradeoff, not a reason to cut early).
inline bool blockIsComplete(std::uint64_t blockIndex, std::uint64_t committedCount) {
  return committedCount >= blockBounds(blockIndex).lastSequenceNumber;
}

}  // namespace sequencer::evidence
