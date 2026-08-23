#pragma once

// The on-disk journal format — specification.md §6.2. Two files:
//
//   data file:  a sequence of variable-length JournalRecords, appended
//               one after another, never modified in place.
//   index file: a fixed IndexHeader followed by a fixed-size array of
//               IndexEntry, one per record — entry i (0-based)
//               corresponds to sequence number i+1.
//
// Both are memory-mapped MAP_SHARED (§6.2). This header defines the
// binary layout and the primitive encode/decode helpers; it performs no
// I/O itself.

#include <atomic>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <type_traits>

namespace sequencer::journal {

// Thrown when an index file's magic or version does not match this
// build's expectations (§6.2) — a bad open fails loudly rather than
// silently misreading garbage.
class JournalFormatError : public std::runtime_error {
 public:
  using std::runtime_error::runtime_error;
};

// Bumping this is a breaking on-disk format change.
inline constexpr std::uint32_t kIndexMagic = 0x4a524e31;  // "JRN1", read as LE u32
inline constexpr std::uint32_t kIndexVersion = 1;

// IndexHeader — specification.md §6.2.
//
// `magic` and `version` are checked, in that order, before anything else
// in the file is trusted (§6.2): a mismatch means "not a valid index
// file" and the reader refuses to open it, rather than parsing arbitrary
// bytes as real offsets.
//
// `committedCount` is the *only* synchronization between writer and
// readers (§6.3): the writer release-stores it after a record's data and
// index entry are fully durable; readers acquire-load it and may then
// freely read any entry below the loaded value.
struct IndexHeader {
  std::uint32_t magic;
  std::uint32_t version;
  std::atomic<std::uint32_t> closedCleanly;
  std::uint32_t reserved;  // explicit padding — always zero, never read
  std::atomic<std::uint64_t> committedCount;
};

static_assert(sizeof(IndexHeader) == 24,
              "IndexHeader must not gain implicit padding — the layout is on-disk ABI");
static_assert(std::atomic<std::uint32_t>::is_always_lock_free);
static_assert(std::atomic<std::uint64_t>::is_always_lock_free);

// IndexEntry — specification.md §6.2. Entry i (0-based) <-> sequence
// number i+1: `index[N-1]` locates record N in O(1), which is the entire
// point of the two-file split (§6.1).
struct IndexEntry {
  std::uint64_t byteOffset;
  std::uint32_t entryLength;
  std::uint32_t reserved;  // explicit padding — always zero, never read
};

static_assert(sizeof(IndexEntry) == 16,
              "IndexEntry must not gain implicit padding — the layout is on-disk ABI");
static_assert(std::is_trivially_copyable_v<IndexEntry>);

// JournalRecord field layout — specification.md §6.2:
//
//   sequenceNumber   u64
//   inputLength      u32
//   input            bytes            (exactly inputLength bytes)
//   outputCount      u16
//   outputs          repeated { length: u32, bytes }
//
// Records are variable-length, so this is never overlaid as a struct;
// fields are read and written at explicit byte offsets via the
// primitives below. This also sidesteps any reliance on struct padding
// or endianness assumptions beyond "this process's native byte order",
// which is exactly what specification.md §4.1 already requires of state
// machine state for the same reason.
namespace detail {

inline void putU16(std::byte* dst, std::uint16_t value) noexcept {
  std::memcpy(dst, &value, sizeof(value));
}
inline void putU32(std::byte* dst, std::uint32_t value) noexcept {
  std::memcpy(dst, &value, sizeof(value));
}
inline void putU64(std::byte* dst, std::uint64_t value) noexcept {
  std::memcpy(dst, &value, sizeof(value));
}

inline std::uint16_t getU16(const std::byte* src) noexcept {
  std::uint16_t value;
  std::memcpy(&value, src, sizeof(value));
  return value;
}
inline std::uint32_t getU32(const std::byte* src) noexcept {
  std::uint32_t value;
  std::memcpy(&value, src, sizeof(value));
  return value;
}
inline std::uint64_t getU64(const std::byte* src) noexcept {
  std::uint64_t value;
  std::memcpy(&value, src, sizeof(value));
  return value;
}

}  // namespace detail

}  // namespace sequencer::journal
