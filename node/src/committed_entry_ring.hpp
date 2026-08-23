#pragma once

// The committed-entry ring — specification.md §5.1, §5.4: single-
// producer/single-consumer, lock-free, bounded by the producer's own
// flow control. In the real harness the producer is braft's FSMCaller
// callback thread (via node/raft/'s adapter) and the consumer is the
// one pinned, busy-spinning apply thread; this header knows nothing
// about braft, so it can be built and tested in complete isolation
// (node/tests/committed_entry_ring_test.cpp does exactly that).
//
// Each entry's input bytes are copied into ring-owned, preallocated
// storage at push() time — never a view into the producer's own
// buffer, which is not guaranteed to outlive the callback that
// delivered it. `context` is an opaque pointer the consumer hands back
// unexamined to a completion callback (apply_loop.hpp); the ring itself
// never dereferences it.

#include <atomic>
#include <cstddef>
#include <cstring>
#include <memory>
#include <stdexcept>
#include <thread>

#include <sequencer/payload.hpp>

namespace sequencer::node::detail {

using sequencer::Payload;

struct CommittedEntry {
  Payload input;   // a view into this ring's own storage — valid only
                    // until the next tryPop() call (see class comment
                    // on CommittedEntryRing for why that's safe).
  void* context;
};

class CommittedEntryRing {
 public:
  CommittedEntryRing(std::size_t capacity, std::size_t maxInputSize)
      : capacity_(capacity),
        maxInputSize_(maxInputSize),
        lengths_(std::make_unique<std::uint32_t[]>(capacity)),
        contexts_(std::make_unique<void*[]>(capacity)),
        storage_(std::make_unique<std::byte[]>(capacity * maxInputSize)) {
    if (capacity < 2) {
      throw std::invalid_argument("CommittedEntryRing: capacity must be >= 2");
    }
  }

  CommittedEntryRing(const CommittedEntryRing&) = delete;
  CommittedEntryRing& operator=(const CommittedEntryRing&) = delete;

  // Producer side (single thread). Copies `input`'s bytes into the next
  // slot and publishes it. If the ring is full, spins until the
  // consumer frees a slot — acceptable backpressure here because the
  // producer is braft's own callback thread, never the pinned apply
  // thread this design otherwise keeps free of any blocking (§5.4).
  void push(Payload input, void* context) {
    if (input.size() > maxInputSize_) {
      throw std::length_error("CommittedEntryRing::push: input exceeds maxInputSize");
    }

    const std::size_t head = head_.load(std::memory_order_relaxed);
    std::size_t next = head + 1;
    if (next == capacity_) next = 0;

    while (next == tail_.load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }

    std::memcpy(storage_.get() + head * maxInputSize_, input.data(), input.size());
    lengths_[head] = static_cast<std::uint32_t>(input.size());
    contexts_[head] = context;

    head_.store(next, std::memory_order_release);
  }

  // Consumer side (single thread). Returns false if the ring is empty.
  bool tryPop(CommittedEntry& out) noexcept {
    const std::size_t tail = tail_.load(std::memory_order_relaxed);
    if (tail == head_.load(std::memory_order_acquire)) {
      return false;
    }

    out.input = Payload(storage_.get() + tail * maxInputSize_, lengths_[tail]);
    out.context = contexts_[tail];

    std::size_t next = tail + 1;
    if (next == capacity_) next = 0;
    tail_.store(next, std::memory_order_release);
    return true;
  }

 private:
  const std::size_t capacity_;
  const std::size_t maxInputSize_;
  std::unique_ptr<std::uint32_t[]> lengths_;
  std::unique_ptr<void*[]> contexts_;
  std::unique_ptr<std::byte[]> storage_;

  alignas(64) std::atomic<std::size_t> head_{0};
  alignas(64) std::atomic<std::size_t> tail_{0};
};

}  // namespace sequencer::node::detail
