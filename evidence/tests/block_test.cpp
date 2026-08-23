#include <sequencer/evidence/block.hpp>

#include <gtest/gtest.h>

namespace sequencer::evidence {
namespace {

TEST(Block, FirstBlockCoversSequenceOneThroughBlockSize) {
  EXPECT_EQ(blockIndexForSequence(1), 1u);
  EXPECT_EQ(blockIndexForSequence(1024), 1u);
  EXPECT_EQ(blockIndexForSequence(1025), 2u);

  const BlockBounds bounds = blockBounds(1);
  EXPECT_EQ(bounds.firstSequenceNumber, 1u);
  EXPECT_EQ(bounds.lastSequenceNumber, 1024u);
}

TEST(Block, SecondBlockCoversTheNextRange) {
  EXPECT_EQ(blockIndexForSequence(2048), 2u);
  EXPECT_EQ(blockIndexForSequence(2049), 3u);

  const BlockBounds bounds = blockBounds(2);
  EXPECT_EQ(bounds.firstSequenceNumber, 1025u);
  EXPECT_EQ(bounds.lastSequenceNumber, 2048u);
}

TEST(Block, IsCompleteOnlyOnceEveryEntryThroughItsLastIsCommitted) {
  EXPECT_FALSE(blockIsComplete(1, /*committedCount=*/1023));
  EXPECT_TRUE(blockIsComplete(1, /*committedCount=*/1024));
  EXPECT_TRUE(blockIsComplete(1, /*committedCount=*/5000));
  EXPECT_FALSE(blockIsComplete(2, /*committedCount=*/1024));
}

TEST(Block, SequenceAndBlockIndicesAreOneBased) {
  EXPECT_THROW(blockIndexForSequence(0), std::invalid_argument);
  EXPECT_THROW(blockBounds(0), std::invalid_argument);
}

}  // namespace
}  // namespace sequencer::evidence
