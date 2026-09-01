#pragma once

// The output gateway's broadcast ring — the delivery counterpart of
// node/src/committed_entry_ring.hpp's committed-entry ring, extended
// from single-consumer to N independent readers. One producer (the
// gateway's tailing thread, publishing each record's codec output
// exactly once, in order) and any number of readers (one per connected
// subscriber), each owning a private cursor. This is deliberately NOT
// a competing-consumer (SPMC) queue: every reader sees every entry and
// filters by the entry's routing tag, so readers never contend with
// each other, never write shared state, and the whole structure needs
// only acquire/release — no CAS anywhere.
//
// Overwrite semantics, not backpressure: the producer never waits for
// a reader (a stalled WebSocket client must not stall dissemination to
// everyone else — Fanout's contract is best-effort delivery, see
// output_codec.hpp). A reader that falls a full ring behind gets
// Overrun on its next read and its connection is closed by the
// transport — surfacing the slow consumer instead of hiding it inside
// an unbounded queue, which is exactly where the multi-millisecond
// queueing delays this design replaces used to accumulate (see
// examples/counter/README.md's benchmark section).
//
// Torn reads are prevented per-slot, seqlock style: each slot carries
// a version — odd while the producer is mid-write, even (encoding the
// sequence number it holds) once complete. A reader copies the payload
// out first and only then re-checks the version; a change means the
// producer lapped it mid-copy, and the (garbage) copy is discarded as
// an Overrun. The producer is a single thread, so slot writes never
// race each other — only a lapping producer races a slow reader, and
// the version check catches exactly that.
//
// Slot payload bytes are stored as atomic 64-bit words copied with
// relaxed loads/stores, not a raw memcpy — a classic seqlock's
// concurrent plain-memory copy is a formal data race under the C++
// memory model (ThreadSanitizer flags it, correctly) even though the
// torn value is always discarded. Relaxed atomic word copies express
// "tearing is expected and handled" in a standard-conforming way, and
// compile to plain 8-byte moves on x86 — the cost is rounding each
// slot up to whole words, nothing more.

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>

namespace sequencer {

// Routing tags. A tag is either "broadcast on topic T" (topic id in
// the low bits) or "addressed to session S" (high bit set). Topic
// strings are interned to small integers by TopicRegistry below, on
// the cold registration/first-publish paths only, so the per-record
// hot path — producer tagging and reader filtering — is integer
// compares, never string hashing.
inline constexpr std::uint64_t kSessionTagBit = std::uint64_t{1} << 63;

inline constexpr std::uint64_t makeTopicTag(std::uint32_t topicId) { return topicId; }
inline constexpr std::uint64_t makeSessionTag(std::uint64_t sessionId) { return kSessionTagBit | sessionId; }

class TopicRegistry {
 public:
  // Returns the stable small id for `topic`, interning it on first
  // sight. Called by the producer per broadcast() (the lookup is a
  // hash-map hit after the first call) and by each subscriber once at
  // registration — both fine under a plain mutex.
  std::uint32_t idFor(const std::string& topic) {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = ids_.find(topic);
    if (it != ids_.end()) {
      return it->second;
    }
    const auto id = static_cast<std::uint32_t>(ids_.size() + 1);
    ids_.emplace(topic, id);
    return id;
  }

 private:
  std::mutex mutex_;
  std::unordered_map<std::string, std::uint32_t> ids_;
};

// Where a published entry came from in the journal.
//
// The ring carries this because a transport cannot recover it
// otherwise: an entry is (tag, payload), so by the time bytes reach a
// reader the record that produced them is gone. gateway/fix/ needs it
// to serve a FIX ResendRequest -- specification.md §8.12's reason 1 is
// that the journal IS the resend store, and honouring that means
// knowing which journal record each sent message came from, across a
// restart.
//
// Zero means "not supplied". The producer default keeps it optional for
// callers with nothing meaningful to record, and the three transports
// that do not care simply use the readOne() overload without it.
struct RecordOrigin {
  std::uint64_t journalSequenceNumber = 0;
  std::uint32_t outputIndex = 0;
  // steady_clock microseconds at publish. Lets a reader measure how
  // long an entry waited in the ring before it was delivered, which is
  // the one hop between the journal and the wire that no other counter
  // covers. Zero when the producer did not supply it.
  std::uint64_t publishTimeUs = 0;
};

class BroadcastRing {
 public:
  enum class ReadResult { Ok, Empty, Overrun };

  BroadcastRing(std::size_t capacity, std::size_t maxPayload)
      : capacity_(capacity),
        mask_(capacity - 1),
        // Rounded up to whole 8-byte words — see the file comment's
        // atomic-word-copy paragraph. maxPayload() reports the rounded
        // value; readers size their buffers from it.
        wordsPerSlot_((maxPayload + 7) / 8),
        maxPayload_(wordsPerSlot_ * 8),
        versions_(std::make_unique<std::atomic<std::uint64_t>[]>(capacity)),
        tags_(std::make_unique<std::atomic<std::uint64_t>[]>(capacity)),
        lengths_(std::make_unique<std::atomic<std::uint32_t>[]>(capacity)),
        origins_(std::make_unique<std::atomic<std::uint64_t>[]>(capacity)),
        outputIndices_(std::make_unique<std::atomic<std::uint32_t>[]>(capacity)),
        publishTimes_(std::make_unique<std::atomic<std::uint64_t>[]>(capacity)),
        storage_(std::make_unique<std::atomic<std::uint64_t>[]>(capacity * wordsPerSlot_)) {
    if (capacity < 2 || (capacity & (capacity - 1)) != 0) {
      throw std::invalid_argument("BroadcastRing: capacity must be a power of two >= 2");
    }
    // Version 0 (even, but encoding no sequence — sequence s publishes
    // as 2s+2, never 0) marks "never written"; readers see it as
    // not-yet-visible, which head() gating makes unreachable anyway.
    for (std::size_t i = 0; i < capacity; ++i) {
      versions_[i].store(0, std::memory_order_relaxed);
    }
  }

  BroadcastRing(const BroadcastRing&) = delete;
  BroadcastRing& operator=(const BroadcastRing&) = delete;

  std::size_t maxPayload() const { return maxPayload_; }

  // The next sequence number the producer will publish. A new reader
  // starts its cursor here: live-only delivery from the moment of
  // subscription, matching the transports' existing semantics.
  std::uint64_t head() const { return head_.load(std::memory_order_acquire); }

  // Producer side (single thread). Publishes one tagged payload;
  // never blocks, never waits on any reader.
  void publish(std::uint64_t tag, const std::byte* data, std::size_t size,
                RecordOrigin origin = {}) {
    if (size > maxPayload_) {
      throw std::length_error("BroadcastRing::publish: payload exceeds maxPayload");
    }
    const std::uint64_t seq = head_.load(std::memory_order_relaxed);
    const std::size_t i = seq & mask_;

    versions_[i].store(2 * seq + 1, std::memory_order_release);  // odd: write in progress
    std::atomic<std::uint64_t>* slot = storage_.get() + i * wordsPerSlot_;
    const std::size_t words = (size + 7) / 8;
    for (std::size_t w = 0; w < words; ++w) {
      std::uint64_t word = 0;  // zero-pads the trailing partial word
      const std::size_t n = std::min<std::size_t>(8, size - w * 8);
      std::memcpy(&word, data + w * 8, n);
      slot[w].store(word, std::memory_order_relaxed);
    }
    tags_[i].store(tag, std::memory_order_relaxed);
    lengths_[i].store(static_cast<std::uint32_t>(size), std::memory_order_relaxed);
    // Inside the version protocol, exactly like the tag and length: a
    // reader that paired a payload with a stale origin would attribute
    // a resend to the wrong journal record, which is worse than not
    // having the field at all.
    origins_[i].store(origin.journalSequenceNumber, std::memory_order_relaxed);
    outputIndices_[i].store(origin.outputIndex, std::memory_order_relaxed);
    publishTimes_[i].store(origin.publishTimeUs, std::memory_order_relaxed);
    versions_[i].store(2 * seq + 2, std::memory_order_release);  // even: holds seq

    head_.store(seq + 1, std::memory_order_release);
  }

  // Reader side. `cursor` is the reader's own (initialize from
  // head()); `buf` must hold at least maxPayload() bytes. On Ok the
  // entry at `cursor` has been copied out and `cursor` advanced. On
  // Overrun the producer has lapped this reader — the copy (if any)
  // is garbage and the reader's session should be closed; the cursor
  // is left untouched. Empty means caught up.
  ReadResult readOne(std::uint64_t& cursor, std::uint64_t& tagOut, std::byte* buf,
                      std::uint32_t& lengthOut) {
    RecordOrigin ignored;
    return readOne(cursor, tagOut, buf, lengthOut, ignored);
  }

  // The same, also reporting where the entry came from in the journal.
  // Only gateway/fix/ needs this; the other transports use the form
  // above and are unaffected.
  ReadResult readOne(std::uint64_t& cursor, std::uint64_t& tagOut, std::byte* buf,
                      std::uint32_t& lengthOut, RecordOrigin& originOut) {
    if (head_.load(std::memory_order_acquire) <= cursor) {
      return ReadResult::Empty;
    }
    const std::size_t i = cursor & mask_;
    const std::uint64_t expected = 2 * cursor + 2;
    const std::uint64_t v1 = versions_[i].load(std::memory_order_acquire);
    if (v1 != expected) {
      // head > cursor guarantees this slot was published for `cursor`
      // at some point, so v1 can only be from a LATER lap of the ring
      // (or its odd write-in-progress marker): the producer has
      // overwritten this reader's next entry.
      return ReadResult::Overrun;
    }
    // These may race a lapping producer's stores — every value loaded
    // here is treated as provisional until the version re-check below
    // accepts it. `length` is always SOME value a producer stored, so
    // it never exceeds maxPayload() and the copy below stays in
    // bounds even when stale.
    const std::uint32_t length = lengths_[i].load(std::memory_order_relaxed);
    const std::uint64_t tag = tags_[i].load(std::memory_order_relaxed);
    const std::uint64_t origin = origins_[i].load(std::memory_order_relaxed);
    const std::uint32_t outputIndex = outputIndices_[i].load(std::memory_order_relaxed);
    const std::uint64_t publishTime = publishTimes_[i].load(std::memory_order_relaxed);
    const std::atomic<std::uint64_t>* slot = storage_.get() + i * wordsPerSlot_;
    const std::size_t words = (length + 7) / 8;
    for (std::size_t w = 0; w < words; ++w) {
      const std::uint64_t word = slot[w].load(std::memory_order_relaxed);
      std::memcpy(buf + w * 8, &word, 8);  // buf holds maxPayload(), always a whole-word multiple
    }
    // The version re-check must observe any producer store that
    // happened during the copy above — hence the fence: no load
    // before it may be reordered after, and the relaxed re-load
    // can't be satisfied from a stale value hoisted before the copy.
    std::atomic_thread_fence(std::memory_order_acquire);
    if (versions_[i].load(std::memory_order_relaxed) != v1) {
      return ReadResult::Overrun;  // lapped mid-copy; discard
    }
    tagOut = tag;
    lengthOut = length;
    originOut.journalSequenceNumber = origin;
    originOut.outputIndex = outputIndex;
    originOut.publishTimeUs = publishTime;
    ++cursor;
    return ReadResult::Ok;
  }

 private:
  const std::size_t capacity_;
  const std::size_t mask_;
  const std::size_t wordsPerSlot_;  // initialized before maxPayload_ (declaration order)
  const std::size_t maxPayload_;
  std::unique_ptr<std::atomic<std::uint64_t>[]> versions_;
  std::unique_ptr<std::atomic<std::uint64_t>[]> tags_;
  std::unique_ptr<std::atomic<std::uint32_t>[]> lengths_;
  // Published and re-checked under the same version protocol as the
  // tag and length above -- see publish().
  std::unique_ptr<std::atomic<std::uint64_t>[]> origins_;
  std::unique_ptr<std::atomic<std::uint32_t>[]> outputIndices_;
  std::unique_ptr<std::atomic<std::uint64_t>[]> publishTimes_;
  std::unique_ptr<std::atomic<std::uint64_t>[]> storage_;

  alignas(64) std::atomic<std::uint64_t> head_{0};
};

// Shared reader idle discipline: spin re-checking for a while (the
// sub-millisecond common case at benchmark rates — a new record is
// rarely more than tens of microseconds away), then yield, then park
// in short sleeps. The same spin-then-back-off idiom the relay's
// journal wait uses (gateway/relay/), replacing the flat 5ms polls
// this design retires. reset() on every successful read.
class IdleStrategy {
 public:
  explicit IdleStrategy(int spinIterations) : spinIterations_(spinIterations) {}

  void idle() {
    if (count_ < spinIterations_) {
      ++count_;
      return;  // pure spin: the caller's next head() check is the wait
    }
    if (count_ < spinIterations_ + kYieldIterations) {
      ++count_;
      std::this_thread::yield();
      return;
    }
    std::this_thread::sleep_for(std::chrono::microseconds(50));
  }

  void reset() { count_ = 0; }

 private:
  static constexpr int kYieldIterations = 10;
  const int spinIterations_;
  int count_ = 0;
};

}  // namespace sequencer
