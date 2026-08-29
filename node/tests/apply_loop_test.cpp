// Tests for the pinned apply thread's core loop (specification.md
// §5.1, §5.4): sequence-number minting, journal append order, and the
// completion callback — all braft-agnostic, driven by a synthetic
// producer pushing directly into the ring.

#include "apply_loop.hpp"

#include <sequencer/journal/reader.hpp>

#include <atomic>
#include <cstring>
#include <filesystem>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

namespace sequencer::node::detail {
namespace {

// A trivial running-total state machine: input is an 8-byte signed
// delta, the one output is the new total (also designated) — the same
// shape as specification.md §10's counter example.
class SumStateMachine : public sequencer::StateMachine {
 public:
  void apply(std::uint64_t, Payload input, OutputCollector& outputs) override {
    std::int64_t delta;
    std::memcpy(&delta, input.data(), sizeof(delta));
    total_ += delta;
    outputs.emit(Payload(reinterpret_cast<const std::byte*>(&total_), sizeof(total_)));
    outputs.designateOutput(0);
  }
  void snapshotSave(sequencer::SnapshotWriter&) override {}
  void snapshotLoad(sequencer::SnapshotReader&) override {}

 private:
  std::int64_t total_ = 0;
};

struct CompletionCtx {
  std::atomic<bool> called{false};
  std::uint64_t sequenceNumber = 0;
  std::int64_t designatedTotal = 0;
};

void recordCompletion(void* context, std::uint64_t sequenceNumber, Payload designatedOutput) {
  auto* ctx = static_cast<CompletionCtx*>(context);
  ctx->sequenceNumber = sequenceNumber;
  std::memcpy(&ctx->designatedTotal, designatedOutput.data(), sizeof(ctx->designatedTotal));
  ctx->called.store(true, std::memory_order_release);
}

Payload payloadOf(const std::int64_t& v) {
  return Payload(reinterpret_cast<const std::byte*>(&v), sizeof(v));
}

class ApplyLoopTest : public ::testing::Test {
 protected:
  void SetUp() override {
    std::string tmpl = (std::filesystem::temp_directory_path() / "apply_loop_test_XXXXXX").string();
    ASSERT_NE(::mkdtemp(tmpl.data()), nullptr);
    dir_ = tmpl;
    writer_ = std::make_unique<journal::JournalWriter>(dir_ / "j.data", dir_ / "j.index");
    ring_ = std::make_unique<CommittedEntryRing>(16, 64);
    loop_ = std::make_unique<ApplyLoop>(sm_, *writer_, *ring_, &recordCompletion);
  }

  void TearDown() override { std::filesystem::remove_all(dir_); }

  std::filesystem::path dir_;
  SumStateMachine sm_;
  std::unique_ptr<journal::JournalWriter> writer_;
  std::unique_ptr<CommittedEntryRing> ring_;
  std::unique_ptr<ApplyLoop> loop_;
};

TEST_F(ApplyLoopTest, StepOnEmptyRingReturnsFalse) { EXPECT_FALSE(loop_->step()); }

TEST_F(ApplyLoopTest, StepMintsDenseSequenceNumbersAndAppendsToJournal) {
  std::vector<std::int64_t> deltas = {5, -2, 10};
  std::vector<CompletionCtx> ctxs(deltas.size());
  for (std::size_t i = 0; i < deltas.size(); ++i) {
    ring_->push(payloadOf(deltas[i]), &ctxs[i]);
  }
  for (std::size_t i = 0; i < deltas.size(); ++i) {
    EXPECT_TRUE(loop_->step());
  }
  EXPECT_FALSE(loop_->step());

  EXPECT_TRUE(ctxs[0].called.load());
  EXPECT_EQ(ctxs[0].sequenceNumber, 1u);
  EXPECT_EQ(ctxs[0].designatedTotal, 5);

  EXPECT_TRUE(ctxs[1].called.load());
  EXPECT_EQ(ctxs[1].sequenceNumber, 2u);
  EXPECT_EQ(ctxs[1].designatedTotal, 3);

  EXPECT_TRUE(ctxs[2].called.load());
  EXPECT_EQ(ctxs[2].sequenceNumber, 3u);
  EXPECT_EQ(ctxs[2].designatedTotal, 13);

  writer_->flush(false);
  journal::JournalReader reader(dir_ / "j.data", dir_ / "j.index");
  ASSERT_EQ(reader.committedCount(), 3u);
  journal::RecordView r3 = reader.record(3);
  std::int64_t v;
  std::memcpy(&v, r3.output(0).data(), sizeof(v));
  EXPECT_EQ(v, 13);
}

TEST_F(ApplyLoopTest, RunDrainsConcurrentProducerInSequenceOrder) {
  std::atomic<bool> stop{false};
  std::thread applyThread([&] { loop_->run(stop); });

  constexpr int kCount = 2000;
  std::vector<CompletionCtx> ctxs(kCount);
  for (int i = 0; i < kCount; ++i) {
    static thread_local std::int64_t delta;
    delta = i;
    ring_->push(payloadOf(delta), &ctxs[i]);
  }

  while (!ctxs[kCount - 1].called.load(std::memory_order_acquire)) {
    std::this_thread::yield();
  }
  stop.store(true, std::memory_order_relaxed);
  applyThread.join();

  for (int i = 0; i < kCount; ++i) {
    EXPECT_TRUE(ctxs[i].called.load());
    EXPECT_EQ(ctxs[i].sequenceNumber, static_cast<std::uint64_t>(i + 1));
  }
}

// The non-default, local-development-only park mode (§5.4) must be
// functionally identical to pure spin — only its idle CPU behavior
// differs, which this test does not attempt to measure.
TEST_F(ApplyLoopTest, RunWithParkModeStillDrainsInSequenceOrder) {
  std::atomic<bool> stop{false};
  std::thread applyThread([&] { loop_->run(stop, /*pureSpin=*/false); });

  constexpr int kCount = 200;
  std::vector<CompletionCtx> ctxs(kCount);
  for (int i = 0; i < kCount; ++i) {
    static thread_local std::int64_t delta;
    delta = i;
    ring_->push(payloadOf(delta), &ctxs[i]);
    // A deliberate gap between pushes so the apply thread observes an
    // empty ring (and takes the park branch) between most entries,
    // not just at the very end.
    std::this_thread::sleep_for(std::chrono::microseconds(50));
  }

  while (!ctxs[kCount - 1].called.load(std::memory_order_acquire)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  stop.store(true, std::memory_order_relaxed);
  applyThread.join();

  for (int i = 0; i < kCount; ++i) {
    EXPECT_TRUE(ctxs[i].called.load());
    EXPECT_EQ(ctxs[i].sequenceNumber, static_cast<std::uint64_t>(i + 1));
  }
}

// A full journal must stop the apply loop, not the process. Before
// this, JournalWriter::append threw std::length_error straight out of
// the apply thread with nothing catching it, so the node died through
// std::terminate leaving only "terminate called after throwing an
// instance of 'std::length_error'" -- which cost two benchmark fleets
// before anyone worked out what it meant.
//
// The tiny maxIndexEntries here is the whole trick: it makes
// exhaustion reachable in a unit test rather than after ~268M records.
TEST(ApplyLoopExhaustion, AFullJournalHaltsTheLoopInsteadOfKillingTheProcess) {
  const std::filesystem::path dir =
      std::filesystem::temp_directory_path() / "seq_apply_exhaust_test";
  std::filesystem::remove_all(dir);
  std::filesystem::create_directories(dir);

  journal::JournalOptions options;
  options.maxIndexEntries = 4;
  journal::JournalWriter writer(dir / "j.data", dir / "j.index", options);

  SumStateMachine stateMachine;
  CommittedEntryRing ring(64, std::size_t{64} << 10);
  ApplyLoop loop(stateMachine, writer, ring, &recordCompletion);

  // One more than the journal can hold.
  const int kCount = 5;
  std::vector<CompletionCtx> ctxs(kCount);
  for (int i = 0; i < kCount; ++i) {
    static thread_local std::int64_t delta;
    delta = 1;
    ring.push(payloadOf(delta), &ctxs[i]);
  }

  std::atomic<bool> stop{false};
  std::thread applyThread([&] { loop.run(stop, /*pureSpin=*/false); });
  while (!loop.halted() && !ctxs[3].called.load(std::memory_order_acquire)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  // Give the fifth entry a chance to be the one that overflows.
  for (int i = 0; i < 200 && !loop.halted(); ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  stop.store(true, std::memory_order_relaxed);
  applyThread.join();

  // Reaching here at all is most of the point: the thread came back
  // rather than taking the process down with it.
  EXPECT_TRUE(loop.halted());
  EXPECT_NE(loop.haltReason().find("maxIndexEntries"), std::string::npos)
      << "halt reason should name the limit that was hit: " << loop.haltReason();
  // The records that DID fit are still durable and correctly numbered.
  EXPECT_EQ(writer.committedCount(), 4u);
  for (int i = 0; i < 4; ++i) {
    EXPECT_TRUE(ctxs[i].called.load()) << "entry " << i << " should have completed";
  }
  std::filesystem::remove_all(dir);
}

// fillPercent reports whichever limit is closer to full, so a long run
// can watch the wall approach instead of discovering it.
TEST(ApplyLoopExhaustion, FillPercentTracksTheBindingLimit) {
  const std::filesystem::path dir =
      std::filesystem::temp_directory_path() / "seq_fill_percent_test";
  std::filesystem::remove_all(dir);
  std::filesystem::create_directories(dir);

  journal::JournalOptions options;
  options.maxIndexEntries = 10;
  journal::JournalWriter writer(dir / "j.data", dir / "j.index", options);
  EXPECT_EQ(writer.fillPercent(), 0);

  SumStateMachine stateMachine;
  OutputCollector collector;
  const std::int64_t delta = 1;
  for (int i = 0; i < 5; ++i) {
    collector.reset();
    stateMachine.apply(static_cast<std::uint64_t>(i + 1), payloadOf(delta), collector);
    writer.append(static_cast<std::uint64_t>(i + 1), payloadOf(delta), collector.outputs());
  }
  // Five of ten index entries used; the data file is nowhere near full,
  // so the index is what fillPercent must be reporting.
  EXPECT_EQ(writer.fillPercent(), 50);
  std::filesystem::remove_all(dir);
}

}  // namespace
}  // namespace sequencer::node::detail
