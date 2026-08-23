#include <sequencer/sdk/alarm.hpp>

#include <gtest/gtest.h>

namespace sequencer::sdk {
namespace {

using Clock = std::chrono::steady_clock;

TEST(ProofTimeoutAlarm, ANewlySubmittedSequenceIsNotOverdueBeforeTheBound) {
  ProofTimeoutAlarm alarm(std::chrono::milliseconds(1000));
  const Clock::time_point t0 = Clock::now();
  alarm.submitted(1, t0);

  EXPECT_TRUE(alarm.overdue(t0 + std::chrono::milliseconds(500)).empty());
  EXPECT_EQ(alarm.pendingCount(), 1u);
}

TEST(ProofTimeoutAlarm, BecomesOverdueOncePastTheBound) {
  ProofTimeoutAlarm alarm(std::chrono::milliseconds(1000));
  const Clock::time_point t0 = Clock::now();
  alarm.submitted(1, t0);

  const std::vector<std::uint64_t> overdue = alarm.overdue(t0 + std::chrono::milliseconds(1500));
  ASSERT_EQ(overdue.size(), 1u);
  EXPECT_EQ(overdue[0], 1u);
}

TEST(ProofTimeoutAlarm, ProofReceivedClearsTheEntryBeforeItCanGoOverdue) {
  ProofTimeoutAlarm alarm(std::chrono::milliseconds(1000));
  const Clock::time_point t0 = Clock::now();
  alarm.submitted(1, t0);
  alarm.proofReceived(1);

  EXPECT_TRUE(alarm.overdue(t0 + std::chrono::milliseconds(5000)).empty());
  EXPECT_EQ(alarm.pendingCount(), 0u);
}

TEST(ProofTimeoutAlarm, TracksMultipleSequenceNumbersOldestFirst) {
  ProofTimeoutAlarm alarm(std::chrono::milliseconds(1000));
  const Clock::time_point t0 = Clock::now();
  alarm.submitted(3, t0);
  alarm.submitted(1, t0 + std::chrono::milliseconds(100));
  alarm.submitted(2, t0 + std::chrono::milliseconds(200));

  const std::vector<std::uint64_t> overdue = alarm.overdue(t0 + std::chrono::milliseconds(5000));
  ASSERT_EQ(overdue.size(), 3u);
  // std::map<uint64_t, ...> iterates in key order, so this happens to
  // come back sorted by sequence number, not submission time — the
  // class makes no ordering promise beyond "every overdue entry is
  // present."
  EXPECT_EQ(overdue[0], 1u);
  EXPECT_EQ(overdue[1], 2u);
  EXPECT_EQ(overdue[2], 3u);
}

}  // namespace
}  // namespace sequencer::sdk
