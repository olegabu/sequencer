// BroadcastRing in isolation — no transports, no journal, no chassis:
// the single-writer/N-private-cursor protocol itself (see
// include/sequencer/broadcast_ring.hpp's own file comment), including
// the two properties everything downstream leans on: per-reader FIFO
// across wraparound, and lap detection instead of blocking or silent
// corruption when the producer overruns a slow reader. The
// ProducerAndReadersRace test is the one meant to be re-run under
// -fsanitize=thread.

#include <sequencer/broadcast_ring.hpp>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

namespace sequencer {
namespace {

std::string payloadFor(std::uint64_t seq) { return "payload-" + std::to_string(seq); }

void publishString(BroadcastRing& ring, std::uint64_t tag, const std::string& s) {
  ring.publish(tag, reinterpret_cast<const std::byte*>(s.data()), s.size());
}

struct ReadOut {
  std::uint64_t tag = 0;
  std::string payload;
};

BroadcastRing::ReadResult readString(BroadcastRing& ring, std::uint64_t& cursor, ReadOut& out) {
  std::vector<std::byte> buf(ring.maxPayload());
  std::uint32_t length = 0;
  const auto result = ring.readOne(cursor, out.tag, buf.data(), length);
  if (result == BroadcastRing::ReadResult::Ok) {
    out.payload.assign(reinterpret_cast<const char*>(buf.data()), length);
  }
  return result;
}

TEST(BroadcastRing, EmptyRingReadsEmpty) {
  BroadcastRing ring(8, 64);
  std::uint64_t cursor = ring.head();
  ReadOut out;
  EXPECT_EQ(readString(ring, cursor, out), BroadcastRing::ReadResult::Empty);
}

TEST(BroadcastRing, RejectsNonPowerOfTwoCapacity) {
  EXPECT_THROW(BroadcastRing(6, 64), std::invalid_argument);
  EXPECT_THROW(BroadcastRing(1, 64), std::invalid_argument);
}

TEST(BroadcastRing, OversizedPayloadThrows) {
  BroadcastRing ring(8, 4);
  const std::string big = "too large for four bytes";
  EXPECT_THROW(publishString(ring, makeTopicTag(1), big), std::length_error);
}

TEST(BroadcastRing, PreservesFifoOrderAcrossWraparound) {
  BroadcastRing ring(8, 64);
  std::uint64_t cursor = ring.head();
  // 100 entries through an 8-slot ring: many complete wraps, with the
  // reader keeping pace so no overrun is ever provoked.
  for (std::uint64_t seq = 0; seq < 100; ++seq) {
    publishString(ring, makeTopicTag(7), payloadFor(seq));
    ReadOut out;
    ASSERT_EQ(readString(ring, cursor, out), BroadcastRing::ReadResult::Ok);
    EXPECT_EQ(out.payload, payloadFor(seq));
    EXPECT_EQ(out.tag, makeTopicTag(7));
  }
  ReadOut out;
  EXPECT_EQ(readString(ring, cursor, out), BroadcastRing::ReadResult::Empty);
}

TEST(BroadcastRing, TwoReadersWithIndependentCursorsBothSeeEverything) {
  BroadcastRing ring(16, 64);
  std::uint64_t cursorA = ring.head();
  std::uint64_t cursorB = ring.head();
  for (std::uint64_t seq = 0; seq < 10; ++seq) {
    publishString(ring, makeTopicTag(1), payloadFor(seq));
  }
  for (std::uint64_t seq = 0; seq < 10; ++seq) {
    ReadOut outA;
    ReadOut outB;
    ASSERT_EQ(readString(ring, cursorA, outA), BroadcastRing::ReadResult::Ok);
    ASSERT_EQ(readString(ring, cursorB, outB), BroadcastRing::ReadResult::Ok);
    EXPECT_EQ(outA.payload, payloadFor(seq));
    EXPECT_EQ(outB.payload, payloadFor(seq));
  }
}

TEST(BroadcastRing, NewReaderStartsLiveAtHeadAndMissesHistory) {
  BroadcastRing ring(16, 64);
  publishString(ring, makeTopicTag(1), "history");
  std::uint64_t cursor = ring.head();  // subscribe after the fact
  ReadOut out;
  EXPECT_EQ(readString(ring, cursor, out), BroadcastRing::ReadResult::Empty);
  publishString(ring, makeTopicTag(1), "live");
  ASSERT_EQ(readString(ring, cursor, out), BroadcastRing::ReadResult::Ok);
  EXPECT_EQ(out.payload, "live");
}

TEST(BroadcastRing, StalledReaderIsOverrunNotBlockedOrCorrupted) {
  BroadcastRing ring(4, 64);
  std::uint64_t cursor = ring.head();
  // 10 entries through a 4-slot ring with the reader never moving:
  // the producer must have lapped cursor's slot, and must never have
  // blocked doing so.
  for (std::uint64_t seq = 0; seq < 10; ++seq) {
    publishString(ring, makeTopicTag(1), payloadFor(seq));
  }
  ReadOut out;
  EXPECT_EQ(readString(ring, cursor, out), BroadcastRing::ReadResult::Overrun);
}

TEST(BroadcastRing, SessionAndTopicTagsAreDistinguishable) {
  // A session id equal in value to a topic id must produce a
  // different tag — the high bit is the discriminator.
  EXPECT_NE(makeSessionTag(7), makeTopicTag(7));
  EXPECT_EQ(makeSessionTag(7) & ~kSessionTagBit, std::uint64_t{7});
  EXPECT_TRUE(makeSessionTag(7) & kSessionTagBit);
  EXPECT_FALSE(makeTopicTag(7) & kSessionTagBit);
}

TEST(TopicRegistry, InternsStableIdsPerTopic) {
  TopicRegistry topics;
  const auto totals = topics.idFor("totals");
  const auto alerts = topics.idFor("alerts");
  EXPECT_NE(totals, alerts);
  EXPECT_EQ(topics.idFor("totals"), totals);
  EXPECT_EQ(topics.idFor("alerts"), alerts);
}

// The concurrency tests proper — a real producer thread racing real
// reader threads through many wraparounds. Two tests, split by which
// guarantee they exercise, because an unpaced producer WILL lap a
// reader doing per-entry work (that's the overwrite contract working,
// not a bug — the first version of this test learned that the hard
// way). Both are meant to be re-run under -fsanitize=thread.
//
// 1: exact FIFO. The producer paces itself against the readers'
// published progress so an overrun is impossible, and each reader
// then requires perfect in-order delivery of every entry.
TEST(BroadcastRing, ProducerAndReadersRaceFifoWhenNeverLapped) {
  constexpr std::uint64_t kEntries = 200000;
  constexpr std::size_t kCapacity = 1 << 12;
  BroadcastRing ring(kCapacity, 64);

  std::atomic<bool> failed{false};
  std::atomic<std::uint64_t> progressA{0};
  std::atomic<std::uint64_t> progressB{0};
  auto reader = [&](std::atomic<std::uint64_t>& progress) {
    std::uint64_t cursor = 0;  // == ring.head() before the producer starts
    std::vector<std::byte> buf(ring.maxPayload());
    std::uint64_t expectedSeq = 0;
    IdleStrategy idle(1000);
    while (expectedSeq < kEntries && !failed.load(std::memory_order_relaxed)) {
      std::uint64_t tag = 0;
      std::uint32_t length = 0;
      switch (ring.readOne(cursor, tag, buf.data(), length)) {
        case BroadcastRing::ReadResult::Ok: {
          const std::string got(reinterpret_cast<const char*>(buf.data()), length);
          if (got != payloadFor(expectedSeq) || tag != makeTopicTag(1)) {
            failed.store(true, std::memory_order_relaxed);
            return;
          }
          ++expectedSeq;
          progress.store(expectedSeq, std::memory_order_relaxed);
          idle.reset();
          break;
        }
        case BroadcastRing::ReadResult::Empty:
          idle.idle();
          break;
        case BroadcastRing::ReadResult::Overrun:
          failed.store(true, std::memory_order_relaxed);
          return;
      }
    }
  };

  std::thread readerA([&] { reader(progressA); });
  std::thread readerB([&] { reader(progressB); });
  for (std::uint64_t seq = 0; seq < kEntries && !failed.load(std::memory_order_relaxed); ++seq) {
    // Stay at most half a ring ahead of the slowest reader — overrun
    // becomes impossible, so FIFO can be asserted exactly.
    while (seq - std::min(progressA.load(std::memory_order_relaxed),
                           progressB.load(std::memory_order_relaxed)) >=
           kCapacity / 2) {
      std::this_thread::yield();
    }
    publishString(ring, makeTopicTag(1), payloadFor(seq));
  }
  readerA.join();
  readerB.join();
  EXPECT_FALSE(failed.load()) << "a reader saw a wrong payload/tag or was overrun despite pacing";
}

// 2: integrity under lapping. Tiny ring, unpaced producer — the
// reader gets overrun constantly, and that's the point: every Ok it
// does get must be byte-exact for its cursor's own sequence number
// (the seqlock version re-check is what guarantees this), and every
// lap must surface as Overrun, never as silently torn bytes. On
// Overrun the reader snaps its cursor forward to head() and carries
// on, like a real transport disconnect/resubscribe would.
TEST(BroadcastRing, LappedReaderSeesOverrunsButNeverTornPayloads) {
  constexpr std::uint64_t kEntries = 100000;
  BroadcastRing ring(8, 64);

  std::atomic<bool> producerDone{false};
  std::atomic<bool> failed{false};
  std::atomic<std::uint64_t> overruns{0};
  // The cursor starts HERE, on this thread, before a single entry is
  // published -- not inside the reader.
  //
  // Reading ring.head() in the reader body makes the starting position
  // depend on how fast the thread gets scheduled, and it reliably loses
  // that race: over 24 concurrent runs on a loaded 4-core box the
  // reader started at 0 exactly zero times, at a median of ~50k, and
  // TWICE at 100000 -- past the producer's entire run. A reader that
  // starts past the end sees Empty, finds producerDone already set,
  // and returns having been lapped never, so the overruns assertion
  // below fails. That is the flake this test showed under full-suite
  // load, and it was in the test, not the ring.
  const std::uint64_t startCursor = ring.head();
  std::thread readerThread([&, startCursor] {
    std::uint64_t cursor = startCursor;
    std::vector<std::byte> buf(ring.maxPayload());
    IdleStrategy idle(1000);
    while (!failed.load(std::memory_order_relaxed)) {
      std::uint64_t tag = 0;
      std::uint32_t length = 0;
      const std::uint64_t seqBeingRead = cursor;
      switch (ring.readOne(cursor, tag, buf.data(), length)) {
        case BroadcastRing::ReadResult::Ok: {
          const std::string got(reinterpret_cast<const char*>(buf.data()), length);
          if (got != payloadFor(seqBeingRead)) {
            failed.store(true, std::memory_order_relaxed);  // torn read escaped detection
            return;
          }
          break;
        }
        case BroadcastRing::ReadResult::Empty:
          if (producerDone.load(std::memory_order_relaxed)) {
            return;
          }
          idle.idle();
          break;
        case BroadcastRing::ReadResult::Overrun:
          overruns.fetch_add(1, std::memory_order_relaxed);
          cursor = ring.head();
          break;
      }
    }
  });
  for (std::uint64_t seq = 0; seq < kEntries; ++seq) {
    publishString(ring, makeTopicTag(1), payloadFor(seq));
  }
  producerDone.store(true, std::memory_order_relaxed);
  readerThread.join();
  EXPECT_FALSE(failed.load()) << "an Ok read returned bytes that were not its sequence's own payload";
  EXPECT_GT(overruns.load(), 0u) << "an 8-slot ring under an unpaced producer should have lapped the reader";
}

}  // namespace
// The journal origin rides the same version protocol as the tag and
// payload (broadcast_ring.hpp's publish()). gateway/fix/ uses it to
// serve a FIX ResendRequest from the journal instead of from a message
// store, so pairing a payload with someone else's origin would
// attribute a resend to the wrong record -- worse than not having the
// field.
TEST(BroadcastRing, RecordOriginTravelsWithItsEntry) {
  BroadcastRing ring(8, 64);
  std::uint64_t cursor = ring.head();

  for (std::uint64_t i = 1; i <= 4; ++i) {
    const std::string payload = "record-" + std::to_string(i);
    ring.publish(makeTopicTag(1), reinterpret_cast<const std::byte*>(payload.data()),
                  payload.size(), RecordOrigin{100 + i, static_cast<std::uint32_t>(i % 2)});
  }

  std::vector<std::byte> buffer(ring.maxPayload());
  for (std::uint64_t i = 1; i <= 4; ++i) {
    std::uint64_t tag = 0;
    std::uint32_t length = 0;
    RecordOrigin origin;
    ASSERT_EQ(ring.readOne(cursor, tag, buffer.data(), length, origin),
              BroadcastRing::ReadResult::Ok);
    EXPECT_EQ(origin.journalSequenceNumber, 100 + i);
    EXPECT_EQ(origin.outputIndex, i % 2);
    const std::string got(reinterpret_cast<const char*>(buffer.data()), length);
    EXPECT_EQ(got, "record-" + std::to_string(i))
        << "the origin must belong to THIS payload, not a neighbour's";
  }
}

// A producer that supplies no origin gets zeroes rather than whatever
// the previous lap left in the slot -- the three transports that do not
// use origins publish this way.
TEST(BroadcastRing, AnOmittedOriginDoesNotInheritThePreviousLapsValue) {
  BroadcastRing ring(2, 64);
  const std::string payload = "x";
  ring.publish(makeTopicTag(1), reinterpret_cast<const std::byte*>(payload.data()),
                payload.size(), RecordOrigin{42, 7});
  // Lap the ring twice so the slot is reused.
  std::uint64_t cursor = ring.head();
  ring.publish(makeTopicTag(1), reinterpret_cast<const std::byte*>(payload.data()),
                payload.size());
  ring.publish(makeTopicTag(1), reinterpret_cast<const std::byte*>(payload.data()),
                payload.size());

  std::vector<std::byte> buffer(ring.maxPayload());
  std::uint64_t tag = 0;
  std::uint32_t length = 0;
  RecordOrigin origin{999, 999};
  ASSERT_EQ(ring.readOne(cursor, tag, buffer.data(), length, origin),
            BroadcastRing::ReadResult::Ok);
  EXPECT_EQ(origin.journalSequenceNumber, 0u);
  EXPECT_EQ(origin.outputIndex, 0u);
}

}  // namespace sequencer
