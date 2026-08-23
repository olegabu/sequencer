// Unit and concurrency tests for the committed-entry ring
// (specification.md §5.4). Entirely braft-agnostic — no braft or brpc
// symbol is touched here, exercising committed_entry_ring.hpp exactly
// as node/raft/'s adapter and apply_loop.hpp use it, but in isolation.

#include "committed_entry_ring.hpp"

#include <atomic>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

namespace sequencer::node::detail {
namespace {

Payload bytesOf(const std::string& s) {
  return Payload(reinterpret_cast<const std::byte*>(s.data()), s.size());
}

TEST(CommittedEntryRing, EmptyRingTryPopReturnsFalse) {
  CommittedEntryRing ring(4, 64);
  CommittedEntry entry{};
  EXPECT_FALSE(ring.tryPop(entry));
}

TEST(CommittedEntryRing, SingleEntryRoundTrips) {
  CommittedEntryRing ring(4, 64);
  const std::string in = "hello";
  int marker = 42;
  ring.push(bytesOf(in), &marker);

  CommittedEntry entry{};
  ASSERT_TRUE(ring.tryPop(entry));
  EXPECT_EQ(std::string(reinterpret_cast<const char*>(entry.input.data()), entry.input.size()), in);
  EXPECT_EQ(entry.context, &marker);
  EXPECT_FALSE(ring.tryPop(entry));
}

TEST(CommittedEntryRing, PreservesFifoOrderAcrossWraparound) {
  CommittedEntryRing ring(4, 32);  // small capacity to force wraparound
  std::vector<std::string> pushed;
  std::vector<int> contexts;

  for (int round = 0; round < 20; ++round) {
    const std::string s = "item-" + std::to_string(round);
    pushed.push_back(s);
    contexts.push_back(round);
    ring.push(bytesOf(pushed.back()), &contexts.back());

    CommittedEntry entry{};
    ASSERT_TRUE(ring.tryPop(entry));
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(entry.input.data()), entry.input.size()), s);
    EXPECT_EQ(*static_cast<int*>(entry.context), round);
  }
}

TEST(CommittedEntryRing, OversizedInputThrows) {
  CommittedEntryRing ring(4, 4);
  const std::string tooBig = "way-too-long-for-four-bytes";
  EXPECT_THROW(ring.push(bytesOf(tooBig), nullptr), std::length_error);
}

TEST(CommittedEntryRing, NullContextIsPreserved) {
  CommittedEntryRing ring(4, 16);
  ring.push(bytesOf("x"), nullptr);
  CommittedEntry entry{};
  ASSERT_TRUE(ring.tryPop(entry));
  EXPECT_EQ(entry.context, nullptr);
}

TEST(CommittedEntryRing, ConcurrentProducerConsumerPreservesOrderAndContent) {
  CommittedEntryRing ring(64, 64);
  constexpr int kCount = 50000;
  std::vector<std::string> expected(kCount);
  for (int i = 0; i < kCount; ++i) {
    expected[i] = "entry-" + std::to_string(i);
  }

  std::thread producer([&] {
    for (int i = 0; i < kCount; ++i) {
      ring.push(bytesOf(expected[i]), reinterpret_cast<void*>(static_cast<std::intptr_t>(i)));
    }
  });

  int nextExpected = 0;
  while (nextExpected < kCount) {
    CommittedEntry entry{};
    if (!ring.tryPop(entry)) {
      continue;
    }
    ASSERT_EQ(std::string(reinterpret_cast<const char*>(entry.input.data()), entry.input.size()),
              expected[nextExpected]);
    ASSERT_EQ(static_cast<int>(reinterpret_cast<std::intptr_t>(entry.context)), nextExpected);
    ++nextExpected;
  }

  producer.join();
}

}  // namespace
}  // namespace sequencer::node::detail
