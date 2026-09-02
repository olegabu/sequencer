// The journal-backed FIX::MessageStore (specification.md §8.13).
//
// The claim under test is that a resend can be served without keeping a
// second copy of every outbound message: set() records provenance, and
// get() rebuilds the message from the journal record it names. These
// tests drive the store directly, with a stub body source standing in
// for the journal and codec, so a failure here is the store's and not a
// gateway's.

#include <sequencer/quickfix/journal_message_store.hpp>

#include <quickfix/Message.h>
#include <quickfix/Values.h>

#include <map>
#include <string>

#include <gtest/gtest.h>

namespace sequencer::quickfix {
namespace {

// Stands in for "re-read the journal record and re-run the output
// codec". Deterministic, which is the property the real one has.
class StubBodies : public BodySource {
 public:
  bool bodyFor(std::uint64_t seq, std::uint32_t index, std::string& msgTypeOut,
                std::string& bodyOut) override {
    const auto it = bodies.find({seq, index});
    if (it == bodies.end()) {
      return false;
    }
    msgTypeOut = "U2";
    bodyOut = it->second;
    ++calls;
    return true;
  }
  std::map<std::pair<std::uint64_t, std::uint32_t>, std::string> bodies;
  int calls = 0;
};

class MemorySequences : public SequenceNumberStore {
 public:
  void load(const std::string& key, int& nextSender, int& nextTarget) override {
    nextSender = sender.count(key) ? sender[key] : 1;
    nextTarget = target.count(key) ? target[key] : 1;
  }
  void save(const std::string& key, int nextSender, int nextTarget) override {
    sender[key] = nextSender;
    target[key] = nextTarget;
    ++saves;
  }
  std::map<std::string, int> sender, target;
  int saves = 0;
};

// Builds what QuickFIX would hand set(): a complete outbound message.
std::string outboundMessage(int seqNum, const std::string& sendingTime) {
  FIX::Message message;
  message.getHeader().setField(FIX::BeginString("FIX.4.4"));
  message.getHeader().setField(FIX::MsgType("U2"));
  message.getHeader().setField(FIX::SenderCompID("SEQUENCER"));
  message.getHeader().setField(FIX::TargetCompID("CLIENT"));
  message.getHeader().setField(FIX::MsgSeqNum(seqNum));
  message.getHeader().setField(
      FIX::SendingTime(FIX::UtcTimeStampConvertor::convert(sendingTime)));
  message.setField(FIX::FieldBase(5001, "42"));
  return message.toString();
}

TEST(JournalMessageStore, RebuildsAMessageFromTheJournalWithoutStoringItsBytes) {
  StubBodies bodies;
  bodies.bodies[{7, 0}] = "5001=42\001";
  MemorySequences sequences;
  JournalMessageStore store("FIX.4.4:SEQUENCER->CLIENT", bodies, sequences);

  store.noteOrigin(7, 0);
  ASSERT_TRUE(store.set(11, outboundMessage(11, "20260101-12:00:00.000")));

  std::vector<std::string> out;
  store.get(11, 11, out);

  ASSERT_EQ(out.size(), 1u) << "the message must be recoverable from the journal alone";
  FIX::Message rebuilt;
  rebuilt.setString(out[0], false);
  FIX::MsgSeqNum seqNum;
  rebuilt.getHeader().getField(seqNum);
  EXPECT_EQ(seqNum.getValue(), 11)
      << "a replay must carry the ORIGINAL sequence number; QuickFIX resends it as-is";
  FIX::MsgType msgType;
  rebuilt.getHeader().getField(msgType);
  EXPECT_EQ(msgType.getValue(), "U2");
  EXPECT_NE(out[0].find("5001=42"), std::string::npos)
      << "the body must come back from the codec, not from a stored copy";
  EXPECT_EQ(bodies.calls, 1) << "get() must go to the journal, which is the whole point";
}

TEST(JournalMessageStore, ReturnsARangeInSequenceOrder) {
  StubBodies bodies;
  MemorySequences sequences;
  JournalMessageStore store("k", bodies, sequences);
  for (int i = 0; i < 4; ++i) {
    bodies.bodies[{static_cast<std::uint64_t>(100 + i), 0}] = "5001=" + std::to_string(i) + "\001";
    store.noteOrigin(100 + i, 0);
    ASSERT_TRUE(store.set(20 + i, outboundMessage(20 + i, "20260101-12:00:00.000")));
  }

  std::vector<std::string> out;
  store.get(21, 22, out);
  ASSERT_EQ(out.size(), 2u) << "exactly the requested range, no more";
  for (std::size_t i = 0; i < out.size(); ++i) {
    FIX::Message message;
    message.setString(out[i], false);
    FIX::MsgSeqNum seqNum;
    message.getHeader().getField(seqNum);
    EXPECT_EQ(seqNum.getValue(), static_cast<int>(21 + i)) << "ascending, as a resend requires";
  }
}

// A message whose journal record has gone is OMITTED rather than
// faked. QuickFIX gap-fills whatever the store does not return, which
// is the correct FIX 4.4 answer -- inventing a message would be worse
// than admitting the hole.
TEST(JournalMessageStore, OmitsAMessageWhoseRecordIsGoneSoQuickFixGapFillsIt) {
  StubBodies bodies;
  MemorySequences sequences;
  JournalMessageStore store("k", bodies, sequences);

  store.noteOrigin(500, 0);  // nothing registered in the stub for 500
  ASSERT_TRUE(store.set(30, outboundMessage(30, "20260101-12:00:00.000")));

  std::vector<std::string> out;
  store.get(30, 30, out);
  EXPECT_TRUE(out.empty())
      << "an unrecoverable message must be left for QuickFIX to gap-fill";
}

// Administrative messages have no journal record behind them -- the
// gateway never proposed them -- so they are omitted too, which is
// exactly what FIX 4.4 says to do with them on a resend.
TEST(JournalMessageStore, OmitsAdministrativeMessagesWhichHaveNoJournalOrigin) {
  StubBodies bodies;
  MemorySequences sequences;
  JournalMessageStore store("k", bodies, sequences);

  // No noteOrigin(): a heartbeat QuickFIX sent on its own initiative.
  ASSERT_TRUE(store.set(40, outboundMessage(40, "20260101-12:00:00.000")));

  std::vector<std::string> out;
  store.get(40, 40, out);
  EXPECT_TRUE(out.empty()) << "an admin message is gap-filled, never replayed";
}

TEST(JournalMessageStore, SequenceNumbersPersistThroughTheStoreAndSurviveRefresh) {
  StubBodies bodies;
  MemorySequences sequences;
  {
    JournalMessageStore store("session-a", bodies, sequences);
    EXPECT_EQ(store.getNextSenderMsgSeqNum(), 1);
    store.setNextSenderMsgSeqNum(17);
    store.setNextTargetMsgSeqNum(23);
    store.incrNextSenderMsgSeqNum();
  }
  // A second store for the same session is what a restart looks like.
  JournalMessageStore reopened("session-a", bodies, sequences);
  EXPECT_EQ(reopened.getNextSenderMsgSeqNum(), 18)
      << "the sender counter must outlive the process -- it is the only session "
          "state that has to";
  EXPECT_EQ(reopened.getNextTargetMsgSeqNum(), 23);
}

TEST(JournalMessageStore, ResetClearsBothCountersAndTheProvenanceMap) {
  StubBodies bodies;
  bodies.bodies[{9, 0}] = "5001=1\001";
  MemorySequences sequences;
  JournalMessageStore store("k", bodies, sequences);

  store.noteOrigin(9, 0);
  ASSERT_TRUE(store.set(5, outboundMessage(5, "20260101-12:00:00.000")));
  store.setNextSenderMsgSeqNum(9);
  store.reset();

  EXPECT_EQ(store.getNextSenderMsgSeqNum(), 1);
  EXPECT_EQ(store.getNextTargetMsgSeqNum(), 1);
  std::vector<std::string> out;
  store.get(1, 100, out);
  EXPECT_TRUE(out.empty()) << "reset must forget what was sent, as ResetSeqNumFlag means";
}

}  // namespace
}  // namespace sequencer::quickfix
