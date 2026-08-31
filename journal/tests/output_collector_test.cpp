// OutputCollector's designation semantics — specification.md §4.
//
// Designation went plural on 2026-08-31: a state machine may mark any
// number of its emitted outputs as belonging to the submitting client's
// synchronous reply. The property that needs guarding is the ORDERING
// rule, because it is the one a caller would naturally get wrong:
// §5.2 requires the reply to preserve EMISSION order, not the order
// designateOutput() happened to be called in.

#include <sequencer/state_machine.hpp>

#include <array>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <vector>

#include <sequencer/journal/record_view.hpp>

#include <gtest/gtest.h>

namespace sequencer {
namespace {

// Stable backing bytes: OutputCollector is non-owning, so what it holds
// must outlive it (specification.md §4's "reused arena" note).
struct Outputs {
  std::array<std::string, 8> storage{"zero", "one", "two", "three", "four", "five", "six", "seven"};

  Payload at(std::size_t i) const {
    return Payload(reinterpret_cast<const std::byte*>(storage[i].data()), storage[i].size());
  }
};

std::string asString(Payload p) {
  return std::string(reinterpret_cast<const char*>(p.data()), p.size());
}

TEST(OutputCollector, DesignatingNothingYieldsAnEmptySet) {
  Outputs bytes;
  OutputCollector collector;
  collector.emit(bytes.at(0));
  collector.emit(bytes.at(1));
  EXPECT_TRUE(collector.designatedOutputs().empty());
  // Emitting is unaffected: everything still goes to the journal and
  // the output gateways regardless of designation.
  EXPECT_EQ(collector.outputs().size(), 2u);
}

TEST(OutputCollector, DesignatingOneYieldsExactlyThatOutput) {
  Outputs bytes;
  OutputCollector collector;
  collector.emit(bytes.at(0));
  collector.emit(bytes.at(1));
  collector.designateOutput(1);
  std::span<const Payload> designated = collector.designatedOutputs();
  ASSERT_EQ(designated.size(), 1u);
  EXPECT_EQ(asString(designated[0]), "one");
}

TEST(OutputCollector, DesignatingManyYieldsAllOfThem) {
  Outputs bytes;
  OutputCollector collector;
  for (std::size_t i = 0; i < 4; ++i) {
    collector.emit(bytes.at(i));
  }
  collector.designateOutput(0);
  collector.designateOutput(1);
  collector.designateOutput(2);
  collector.designateOutput(3);
  std::span<const Payload> designated = collector.designatedOutputs();
  ASSERT_EQ(designated.size(), 4u);
  EXPECT_EQ(asString(designated[0]), "zero");
  EXPECT_EQ(asString(designated[3]), "three");
}

// The rule that motivates the bitset. An order-matching state machine
// designating an acknowledgement and then fills would notice this
// immediately; a counter designating one output never would.
TEST(OutputCollector, OutOfOrderDesignationStillYieldsEmissionOrder) {
  Outputs bytes;
  OutputCollector collector;
  for (std::size_t i = 0; i < 5; ++i) {
    collector.emit(bytes.at(i));
  }
  collector.designateOutput(4);
  collector.designateOutput(1);
  collector.designateOutput(3);
  std::span<const Payload> designated = collector.designatedOutputs();
  ASSERT_EQ(designated.size(), 3u);
  EXPECT_EQ(asString(designated[0]), "one");
  EXPECT_EQ(asString(designated[1]), "three");
  EXPECT_EQ(asString(designated[2]), "four");
}

TEST(OutputCollector, DesignatingTheSameIndexTwiceIsIdempotent) {
  Outputs bytes;
  OutputCollector collector;
  collector.emit(bytes.at(0));
  collector.emit(bytes.at(1));
  collector.designateOutput(1);
  collector.designateOutput(1);
  collector.designateOutput(1);
  std::span<const Payload> designated = collector.designatedOutputs();
  ASSERT_EQ(designated.size(), 1u);
  EXPECT_EQ(asString(designated[0]), "one");
}

// A hard error, not a silent no-op: designating an index that has not
// been emitted is a state-machine bug, and the alternative is a reply
// that quietly omits what the author believed they had designated.
TEST(OutputCollector, DesignatingAnUnemittedIndexThrows) {
  Outputs bytes;
  OutputCollector collector;
  collector.emit(bytes.at(0));
  EXPECT_THROW(collector.designateOutput(1), std::out_of_range);
  EXPECT_THROW(collector.designateOutput(99), std::out_of_range);
}

// The harness resets between apply() calls; designation must not leak
// from one input's consequences into the next one's reply.
TEST(OutputCollector, ResetClearsDesignationAsWellAsOutputs) {
  Outputs bytes;
  OutputCollector collector;
  collector.emit(bytes.at(0));
  collector.designateOutput(0);
  ASSERT_EQ(collector.designatedOutputs().size(), 1u);

  collector.reset();
  EXPECT_TRUE(collector.outputs().empty());
  EXPECT_TRUE(collector.designatedOutputs().empty());

  collector.emit(bytes.at(2));
  EXPECT_TRUE(collector.designatedOutputs().empty());
}

// Repeated observation must be stable -- designatedOutputs() fills a
// cache, and a second call must not append to the first call's result.
TEST(OutputCollector, DesignatedOutputsIsStableAcrossRepeatedCalls) {
  Outputs bytes;
  OutputCollector collector;
  collector.emit(bytes.at(0));
  collector.emit(bytes.at(1));
  collector.designateOutput(0);
  EXPECT_EQ(collector.designatedOutputs().size(), 1u);
  EXPECT_EQ(collector.designatedOutputs().size(), 1u);
  EXPECT_EQ(asString(collector.designatedOutputs()[0]), "zero");
}

// specification.md §11 (determinism certification) is unaffected by
// designation going plural, and this asserts that rather than assuming
// it: designation is NOT part of the journal record.
//
// The record encodes a sequence number, the input, and every emitted
// output (record_view.hpp's recordEncodedSize/encodeRecord). Nothing
// there varies with which outputs were designated -- so two applies
// that emit identically but designate differently must produce
// byte-identical records, and a replay cannot diverge because of a
// change to designation.
TEST(OutputCollector, DesignationDoesNotAffectTheJournaledRecord) {
  Outputs bytes;

  OutputCollector none;
  OutputCollector some;
  for (std::size_t i = 0; i < 3; ++i) {
    none.emit(bytes.at(i));
    some.emit(bytes.at(i));
  }
  some.designateOutput(0);
  some.designateOutput(2);

  // They differ in what the client is told...
  EXPECT_EQ(none.designatedOutputs().size(), 0u);
  EXPECT_EQ(some.designatedOutputs().size(), 2u);

  // ...and are identical in what is journaled.
  const Payload input = bytes.at(7);
  ASSERT_EQ(journal::recordEncodedSize(input, none.outputs()),
            journal::recordEncodedSize(input, some.outputs()));

  const std::size_t size = journal::recordEncodedSize(input, none.outputs());
  std::vector<std::byte> encodedNone(size);
  std::vector<std::byte> encodedSome(size);
  journal::encodeRecord(encodedNone.data(), 42, input, none.outputs());
  journal::encodeRecord(encodedSome.data(), 42, input, some.outputs());
  EXPECT_EQ(encodedNone, encodedSome)
      << "designation leaked into the journal; §11 replay would no longer be "
         "independent of it";
}

}  // namespace
}  // namespace sequencer
