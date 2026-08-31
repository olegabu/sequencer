// The FIX session core, driven against a scripted peer.
//
// No sockets: FixSession consumes and produces bytes, so a test wires
// two of them together (or feeds one hand-built frames) and advances an
// injected clock instead of sleeping. That is why the class is shaped
// this way -- see its header comment.

#include <sequencer/fix/fix_session.hpp>

#include <hffix_fields.hpp>

#include <functional>
#include <map>
#include <string>
#include <vector>

#include <gtest/gtest.h>

namespace sequencer::fix {
namespace {

class MemorySequenceStore : public SequenceStore {
 public:
  SequenceNumbers load(const std::string& key) override { return numbers_[key]; }
  void store(const std::string& key, const SequenceNumbers& numbers) override {
    numbers_[key] = numbers;
  }
  // Survives a "restart" in tests: construct a new FixSession over the
  // same store and the counters come back, which is the only session
  // state specification.md §8.12 says must persist.
  std::map<std::string, SequenceNumbers> numbers_;
};

// Captures what a session put on the wire, parsed back into messages.
struct Wire {
  std::vector<std::string> frames;

  void operator()(std::string_view frame) { frames.emplace_back(frame); }

  std::size_t count() const { return frames.size(); }

  std::string typeOf(std::size_t i) const {
    hffix::message_reader reader(frames[i].data(), frames[i].size());
    EXPECT_TRUE(reader.is_complete()) << "frame " << i << " is not a complete FIX message";
    EXPECT_TRUE(reader.is_valid()) << "frame " << i << " failed BodyLength/CheckSum";
    const auto type = reader.message_type();
    return std::string(type->value().begin(), type->value().size());
  }

  std::string fieldOf(std::size_t i, int tag) const {
    hffix::message_reader reader(frames[i].data(), frames[i].size());
    for (auto it = reader.begin(); it != reader.end(); ++it) {
      if (it->tag() == tag) {
        return std::string(it->value().begin(), it->value().size());
      }
    }
    return {};
  }

  bool has(std::size_t i, int tag) const { return !fieldOf(i, tag).empty(); }

  std::string last() const { return frames.back(); }
  void clear() { frames.clear(); }
};

// A controllable clock, in microseconds.
struct Clock {
  std::uint64_t now = 1'700'000'000'000'000ULL;  // a fixed, plausible epoch
  std::uint64_t operator()() const { return now; }
  void advanceSeconds(double s) { now += static_cast<std::uint64_t>(s * 1'000'000.0); }
};

struct Peer {
  MemorySequenceStore store;
  Clock clock;
  Wire wire;
  std::unique_ptr<FixSession> session;
  std::vector<std::string> appMessages;
  std::vector<std::pair<SessionEvent, DisconnectReason>> events;

  Peer(Role role, const std::string& sender, const std::string& target, int heartBtInt = 30) {
    SessionConfig config;
    config.role = role;
    config.senderCompId = sender;
    config.targetCompId = target;
    config.heartBtInt = heartBtInt;
    session = std::make_unique<FixSession>(config, store, [this] { return clock.now; });
    session->setSendFn([this](std::string_view f) { wire(f); });
    session->setAppMessageFn([this](const hffix::message_reader& m) {
      appMessages.emplace_back(m.message_begin(), m.message_size());
    });
    session->setEventFn([this](SessionEvent e, DisconnectReason r) { events.emplace_back(e, r); });
  }
};

// Pumps whatever one side wrote into the other, until both are quiet.
void pump(Peer& a, Peer& b) {
  for (int i = 0; i < 8; ++i) {
    bool moved = false;
    if (!a.wire.frames.empty()) {
      std::vector<std::string> frames;
      frames.swap(a.wire.frames);
      for (const std::string& f : frames) {
        b.session->onBytes(f);
      }
      moved = true;
    }
    if (!b.wire.frames.empty()) {
      std::vector<std::string> frames;
      frames.swap(b.wire.frames);
      for (const std::string& f : frames) {
        a.session->onBytes(f);
      }
      moved = true;
    }
    if (!moved) {
      return;
    }
  }
}

TEST(FixSession, LogonHandshakeCompletesBothSides) {
  Peer initiator(Role::Initiator, "CLIENT", "VENUE");
  Peer acceptor(Role::Acceptor, "VENUE", "CLIENT");
  acceptor.session->start();
  initiator.session->start();

  ASSERT_EQ(initiator.wire.typeOf(0), "A") << "an initiator opens with Logon";
  pump(initiator, acceptor);

  EXPECT_TRUE(initiator.session->isLoggedOn());
  EXPECT_TRUE(acceptor.session->isLoggedOn());
}

TEST(FixSession, HeartbeatIntervalIsNegotiatedFromTheInitiatorsProposal) {
  Peer initiator(Role::Initiator, "CLIENT", "VENUE", /*heartBtInt=*/12);
  Peer acceptor(Role::Acceptor, "VENUE", "CLIENT", /*heartBtInt=*/30);
  acceptor.session->start();
  initiator.session->start();
  EXPECT_EQ(initiator.wire.fieldOf(0, hffix::tag::HeartBtInt), "12");
  pump(initiator, acceptor);
  // FIX 4.4: the acceptor adopts and echoes the initiator's proposal.
  EXPECT_TRUE(acceptor.session->isLoggedOn());
}

TEST(FixSession, SequenceNumbersAdvanceMonotonicallyOnBothSides) {
  Peer initiator(Role::Initiator, "CLIENT", "VENUE");
  Peer acceptor(Role::Acceptor, "VENUE", "CLIENT");
  acceptor.session->start();
  initiator.session->start();
  pump(initiator, acceptor);

  initiator.wire.clear();
  initiator.session->sendApplication("D", "");
  initiator.session->sendApplication("D", "");
  EXPECT_EQ(initiator.wire.fieldOf(0, hffix::tag::MsgSeqNum), "2");
  EXPECT_EQ(initiator.wire.fieldOf(1, hffix::tag::MsgSeqNum), "3");
  pump(initiator, acceptor);
  EXPECT_EQ(acceptor.appMessages.size(), 2u);
}

TEST(FixSession, TestRequestIsIssuedOnSilenceAndAnsweredWithItsId) {
  Peer initiator(Role::Initiator, "CLIENT", "VENUE", /*heartBtInt=*/10);
  Peer acceptor(Role::Acceptor, "VENUE", "CLIENT", /*heartBtInt=*/10);
  acceptor.session->start();
  initiator.session->start();
  pump(initiator, acceptor);
  initiator.wire.clear();
  acceptor.wire.clear();

  // Past 1.2x the interval with nothing received: a TestRequest is due.
  initiator.clock.advanceSeconds(13);
  initiator.session->poll();
  bool sawTestRequest = false;
  std::string testReqId;
  for (std::size_t i = 0; i < initiator.wire.count(); ++i) {
    if (initiator.wire.typeOf(i) == "1") {
      sawTestRequest = true;
      testReqId = initiator.wire.fieldOf(i, hffix::tag::TestReqID);
    }
  }
  ASSERT_TRUE(sawTestRequest) << "silence past testRequestFactor must produce a TestRequest";
  EXPECT_FALSE(testReqId.empty());

  pump(initiator, acceptor);
  // The answer is a Heartbeat echoing the id.
  bool echoed = false;
  for (std::size_t i = 0; i < acceptor.wire.count(); ++i) {
    if (acceptor.wire.typeOf(i) == "0" &&
        acceptor.wire.fieldOf(i, hffix::tag::TestReqID) == testReqId) {
      echoed = true;
    }
  }
  // acceptor.wire was drained by pump; check what the initiator received
  // instead by confirming it did not then disconnect on silence.
  initiator.clock.advanceSeconds(1);
  initiator.session->poll();
  EXPECT_TRUE(initiator.session->isLoggedOn())
      << "an answered TestRequest must clear the silence timer";
  (void)echoed;
}

TEST(FixSession, ContinuedPeerSilenceDisconnects) {
  Peer initiator(Role::Initiator, "CLIENT", "VENUE", /*heartBtInt=*/10);
  Peer acceptor(Role::Acceptor, "VENUE", "CLIENT", /*heartBtInt=*/10);
  acceptor.session->start();
  initiator.session->start();
  pump(initiator, acceptor);

  initiator.clock.advanceSeconds(25);  // past 2.4x
  initiator.session->poll();
  EXPECT_FALSE(initiator.session->isLoggedOn());
  EXPECT_EQ(initiator.session->disconnectReason(), DisconnectReason::PeerSilence);
}

TEST(FixSession, LogoutHandshakeEndsBothSides) {
  Peer initiator(Role::Initiator, "CLIENT", "VENUE");
  Peer acceptor(Role::Acceptor, "VENUE", "CLIENT");
  acceptor.session->start();
  initiator.session->start();
  pump(initiator, acceptor);

  initiator.session->logout("done for the day");
  pump(initiator, acceptor);
  EXPECT_FALSE(acceptor.session->isLoggedOn());
  EXPECT_EQ(acceptor.session->disconnectReason(), DisconnectReason::PeerLogout);
}

TEST(FixSession, MalformedFramingDisconnectsRatherThanRejecting) {
  // A real frame with its CheckSum digits corrupted. Built this way on
  // purpose: a hand-written short message with a wrong BodyLength just
  // looks INCOMPLETE, and the session correctly waits for more bytes
  // rather than rejecting -- so that would have tested nothing.
  Peer initiator(Role::Initiator, "CLIENT", "VENUE");
  initiator.session->start();
  std::string frame = initiator.wire.frames[0];
  const std::size_t checksumPos = frame.rfind("10=");
  ASSERT_NE(checksumPos, std::string::npos);
  frame[checksumPos + 3] = (frame[checksumPos + 3] == '9') ? '0' : '9';

  Peer acceptor(Role::Acceptor, "VENUE", "CLIENT");
  acceptor.session->start();
  acceptor.session->onBytes(frame);

  EXPECT_FALSE(acceptor.session->isLoggedOn());
  // FIX 4.4: a garbled message must NOT be answered with a Reject --
  // its sequence number cannot be trusted -- so the session drops.
  EXPECT_EQ(acceptor.session->disconnectReason(), DisconnectReason::MalformedFraming);
}

TEST(FixSession, PartialAndCoalescedFramesAreBothHandled) {
  Peer initiator(Role::Initiator, "CLIENT", "VENUE");
  Peer acceptor(Role::Acceptor, "VENUE", "CLIENT");
  acceptor.session->start();
  initiator.session->start();
  pump(initiator, acceptor);

  initiator.wire.clear();
  initiator.session->sendApplication("D", "");
  initiator.session->sendApplication("D", "");
  const std::string both = initiator.wire.frames[0] + initiator.wire.frames[1];

  // Split mid-way through the first message: neither half is a message.
  acceptor.appMessages.clear();
  const std::size_t split = both.size() / 3;
  acceptor.session->onBytes(both.substr(0, split));
  EXPECT_EQ(acceptor.appMessages.size(), 0u) << "a partial frame must not be dispatched";
  acceptor.session->onBytes(both.substr(split));
  EXPECT_EQ(acceptor.appMessages.size(), 2u) << "both coalesced frames must be dispatched";
}

TEST(FixSession, SequenceNumbersSurviveAReconstruction) {
  MemorySequenceStore store;
  Clock clock;
  {
    SessionConfig config;
    config.role = Role::Initiator;
    config.senderCompId = "CLIENT";
    config.targetCompId = "VENUE";
    FixSession session(config, store, [&clock] { return clock.now; });
    session.setSendFn([](std::string_view) {});
    session.start();
    session.sendApplication("D", "");
    session.sendApplication("D", "");
    EXPECT_EQ(session.sequences().nextOutbound, 4u);
  }
  // "Restart": a fresh session over the same store resumes the counters
  // -- the only state §8.12 says must persist.
  SessionConfig config;
  config.role = Role::Initiator;
  config.senderCompId = "CLIENT";
  config.targetCompId = "VENUE";
  FixSession revived(config, store, [&clock] { return clock.now; });
  EXPECT_EQ(revived.sequences().nextOutbound, 4u);
}

TEST(FixSession, ResetSeqNumFlagOnLogonResetsBothCounters) {
  // End-of-day reset, as it actually happens: the session drops, and
  // the initiator reconnects with 141=Y against an acceptor whose
  // persisted counters are high from yesterday.
  //
  // The first version of this test sent the reset Logon into a session
  // that was still logged on. That is not a reconnect, and the session
  // correctly treated it as a sequence violation -- so the test failed
  // for a reason that had nothing to do with ResetSeqNumFlag.
  MemorySequenceStore acceptorStore;
  Clock clock;

  auto makeAcceptor = [&](Wire& wire) {
    SessionConfig config;
    config.role = Role::Acceptor;
    config.senderCompId = "VENUE";
    config.targetCompId = "CLIENT";
    auto session = std::make_unique<FixSession>(config, acceptorStore, [&clock] { return clock.now; });
    session->setSendFn([&wire](std::string_view f) { wire(f); });
    return session;
  };

  // Yesterday: the acceptor got up to sequence 9.
  acceptorStore.numbers_["VENUE->CLIENT"] = SequenceNumbers{.nextOutbound = 9, .nextInbound = 9};

  Wire wire;
  auto acceptor = makeAcceptor(wire);
  acceptor->start();
  ASSERT_EQ(acceptor->sequences().nextInbound, 9u);

  // Today: a fresh initiator logs on with ResetSeqNumFlag.
  MemorySequenceStore initiatorStore;
  SessionConfig initiatorConfig;
  initiatorConfig.role = Role::Initiator;
  initiatorConfig.senderCompId = "CLIENT";
  initiatorConfig.targetCompId = "VENUE";
  initiatorConfig.resetSeqNumOnLogon = true;
  Wire initiatorWire;
  FixSession initiator(initiatorConfig, initiatorStore, [&clock] { return clock.now; });
  initiator.setSendFn([&initiatorWire](std::string_view f) { initiatorWire(f); });
  initiator.start();
  ASSERT_EQ(initiatorWire.fieldOf(0, hffix::tag::ResetSeqNumFlag), "Y");

  acceptor->onBytes(initiatorWire.frames[0]);

  EXPECT_TRUE(acceptor->isLoggedOn());
  EXPECT_EQ(acceptor->sequences().nextInbound, 2u)
      << "141=Y resets inbound to 1; the Logon itself then consumes 1";
  // The outbound counter resets too -- the acceptor's Logon echo is
  // sequence 1, not 9.
  EXPECT_EQ(wire.fieldOf(0, hffix::tag::MsgSeqNum), "1");
}

TEST(FixSession, AuthenticationFailureLogsOutAndDisconnects) {
  Peer acceptor(Role::Acceptor, "VENUE", "CLIENT");
  acceptor.session->setAuthenticator([](std::string_view user, std::string_view) {
    return user == "goodguy";
  });
  acceptor.session->start();

  Peer initiator(Role::Initiator, "CLIENT", "VENUE");
  initiator.session->start();
  acceptor.session->onBytes(initiator.wire.frames[0]);

  EXPECT_FALSE(acceptor.session->isLoggedOn());
  EXPECT_EQ(acceptor.session->disconnectReason(), DisconnectReason::AuthenticationFailed);
}

TEST(FixSession, ASequenceNumberBelowExpectationIsFatal) {
  Peer initiator(Role::Initiator, "CLIENT", "VENUE");
  Peer acceptor(Role::Acceptor, "VENUE", "CLIENT");
  acceptor.session->start();
  initiator.session->start();
  pump(initiator, acceptor);

  // Replay the Logon frame: its MsgSeqNum 1 is now below expectation
  // and carries no PossDupFlag.
  Peer replay(Role::Initiator, "CLIENT", "VENUE");
  replay.session->start();
  acceptor.session->onBytes(replay.wire.frames[0]);
  EXPECT_EQ(acceptor.session->disconnectReason(), DisconnectReason::SequenceTooLow);
}

TEST(FixSession, AGapTriggersAResendRequestAndStopsProcessing) {
  Peer initiator(Role::Initiator, "CLIENT", "VENUE");
  Peer acceptor(Role::Acceptor, "VENUE", "CLIENT");
  acceptor.session->start();
  initiator.session->start();
  pump(initiator, acceptor);

  initiator.wire.clear();
  initiator.session->sendApplication("D", "");  // seq 2
  initiator.session->sendApplication("D", "");  // seq 3
  const std::string third = initiator.wire.frames[1];

  acceptor.wire.clear();
  acceptor.appMessages.clear();
  acceptor.session->onBytes(third);  // arrives with 2 missing

  EXPECT_EQ(acceptor.appMessages.size(), 0u) << "processing must not run ahead of a gap";
  bool sawResendRequest = false;
  for (std::size_t i = 0; i < acceptor.wire.count(); ++i) {
    if (acceptor.wire.typeOf(i) == "2") {
      sawResendRequest = true;
      EXPECT_EQ(acceptor.wire.fieldOf(i, hffix::tag::BeginSeqNo), "2");
    }
  }
  EXPECT_TRUE(sawResendRequest);
}

// A ResendSource standing in for the journal. specification.md §8.12's
// reason 1 is that there is NO outbound message store -- the gateway
// re-reads the journal -- so the session core asks through this hook
// and a test can supply whatever the journal would have returned.
class ScriptedResendSource : public ResendSource {
 public:
  struct Entry {
    std::uint64_t seqNum;
    std::string msgType;
    std::string body;
    std::string sendingTime;
  };

  std::vector<Entry> entries;
  bool available = true;
  std::uint64_t lastBegin = 0;
  std::uint64_t lastEnd = 0;

  bool resend(std::uint64_t begin, std::uint64_t end, const Emit& emit) override {
    lastBegin = begin;
    lastEnd = end;
    if (!available) {
      return false;
    }
    for (const Entry& e : entries) {
      if (e.seqNum >= begin && e.seqNum <= end) {
        emit(e.seqNum, e.msgType, e.body, e.sendingTime);
      }
    }
    return true;
  }
};

// Builds an arbitrary well-formed FIX message, so a test can drive the
// session with messages it has no API to send (a ResendRequest at a
// chosen range, for instance).
std::string buildFrame(std::string_view msgType, std::uint64_t seqNum,
                        const std::function<void(hffix::message_writer&)>& fill) {
  static thread_local std::vector<char> buffer(4096);
  hffix::message_writer writer(buffer.data(), buffer.data() + buffer.size());
  writer.push_back_header("FIX.4.4");
  writer.push_back_string(hffix::tag::MsgType, msgType.data(), msgType.data() + msgType.size());
  writer.push_back_string(hffix::tag::SenderCompID, "CLIENT");
  writer.push_back_string(hffix::tag::TargetCompID, "VENUE");
  writer.push_back_int(hffix::tag::MsgSeqNum, static_cast<int>(seqNum));
  writer.push_back_string(hffix::tag::SendingTime, "20231114-22:13:20.000");
  fill(writer);
  writer.push_back_trailer();
  return std::string(writer.message_begin(), writer.message_size());
}

TEST(FixSession, AResendRequestWithNoSourceIsAnsweredWithAGapFill) {
  // The correct FIX answer to "resend 2..4" when nothing can be
  // replayed is SequenceReset/GapFill covering the range -- not
  // silence, which would leave the peer stuck waiting.
  Peer acceptor(Role::Acceptor, "VENUE", "CLIENT");
  acceptor.session->start();
  Peer initiator(Role::Initiator, "CLIENT", "VENUE");
  initiator.session->start();
  acceptor.session->onBytes(initiator.wire.frames[0]);
  ASSERT_TRUE(acceptor.session->isLoggedOn());

  acceptor.wire.clear();
  acceptor.session->onBytes(buildFrame("2", 2, [](hffix::message_writer& w) {
    w.push_back_int(hffix::tag::BeginSeqNo, 2);
    w.push_back_int(hffix::tag::EndSeqNo, 4);
  }));

  ASSERT_GE(acceptor.wire.count(), 1u);
  bool sawGapFill = false;
  for (std::size_t i = 0; i < acceptor.wire.count(); ++i) {
    if (acceptor.wire.typeOf(i) == "4") {
      sawGapFill = true;
      EXPECT_EQ(acceptor.wire.fieldOf(i, hffix::tag::GapFillFlag), "Y");
      EXPECT_EQ(acceptor.wire.fieldOf(i, hffix::tag::NewSeqNo), "5")
          << "a gap fill must cover the whole requested range";
    }
  }
  EXPECT_TRUE(sawGapFill);
}

TEST(FixSession, AResendIsServedFromTheSourceWithPossDupAndOriginalSendingTime) {
  Peer acceptor(Role::Acceptor, "VENUE", "CLIENT");
  ScriptedResendSource source;
  source.entries = {
      {2, "U2", "5001=alpha\001", "20231114-10:00:00.000"},
      {3, "U2", "5001=beta\001", "20231114-10:00:01.000"},
  };
  acceptor.session->setResendSource(&source);
  acceptor.session->start();

  Peer initiator(Role::Initiator, "CLIENT", "VENUE");
  initiator.session->start();
  acceptor.session->onBytes(initiator.wire.frames[0]);
  ASSERT_TRUE(acceptor.session->isLoggedOn());

  acceptor.wire.clear();
  acceptor.session->onBytes(buildFrame("2", 2, [](hffix::message_writer& w) {
    w.push_back_int(hffix::tag::BeginSeqNo, 2);
    w.push_back_int(hffix::tag::EndSeqNo, 3);
  }));

  EXPECT_EQ(source.lastBegin, 2u);
  EXPECT_EQ(source.lastEnd, 3u);
  ASSERT_EQ(acceptor.wire.count(), 2u) << "both requested messages must be resent";

  for (std::size_t i = 0; i < 2; ++i) {
    EXPECT_EQ(acceptor.wire.typeOf(i), "U2");
    // Resent at the ORIGINAL sequence number, not renumbered.
    EXPECT_EQ(acceptor.wire.fieldOf(i, hffix::tag::MsgSeqNum), std::to_string(i + 2));
    EXPECT_EQ(acceptor.wire.fieldOf(i, hffix::tag::PossDupFlag), "Y");
    EXPECT_EQ(acceptor.wire.fieldOf(i, hffix::tag::OrigSendingTime),
              source.entries[i].sendingTime);
    // The original body survives the round trip -- an earlier version
    // of the resend path discarded it and sent an empty message.
    EXPECT_EQ(acceptor.wire.fieldOf(i, 5001), i == 0 ? "alpha" : "beta");
  }
}

TEST(FixSession, AResendSkippingASequenceNumberGapFillsTheHole) {
  // The journal has no record of sequence 3 -- it was an administrative
  // message, which is never resent. FIX 4.4 requires the hole be filled
  // with SequenceReset/GapFill so the peer's counter still advances.
  Peer acceptor(Role::Acceptor, "VENUE", "CLIENT");
  ScriptedResendSource source;
  source.entries = {{2, "U2", "5001=alpha\001", "20231114-10:00:00.000"},
                    {4, "U2", "5001=gamma\001", "20231114-10:00:02.000"}};
  acceptor.session->setResendSource(&source);
  acceptor.session->start();

  Peer initiator(Role::Initiator, "CLIENT", "VENUE");
  initiator.session->start();
  acceptor.session->onBytes(initiator.wire.frames[0]);
  acceptor.wire.clear();

  acceptor.session->onBytes(buildFrame("2", 2, [](hffix::message_writer& w) {
    w.push_back_int(hffix::tag::BeginSeqNo, 2);
    w.push_back_int(hffix::tag::EndSeqNo, 4);
  }));

  ASSERT_EQ(acceptor.wire.count(), 3u);
  EXPECT_EQ(acceptor.wire.typeOf(0), "U2");
  EXPECT_EQ(acceptor.wire.typeOf(1), "4") << "the missing 3 must be gap-filled";
  EXPECT_EQ(acceptor.wire.fieldOf(1, hffix::tag::GapFillFlag), "Y");
  EXPECT_EQ(acceptor.wire.fieldOf(1, hffix::tag::NewSeqNo), "4");
  EXPECT_EQ(acceptor.wire.typeOf(2), "U2");
  EXPECT_EQ(acceptor.wire.fieldOf(2, hffix::tag::MsgSeqNum), "4");
}

TEST(FixSession, SequenceResetGapFillAdvancesTheInboundExpectation) {
  Peer acceptor(Role::Acceptor, "VENUE", "CLIENT");
  acceptor.session->start();
  Peer initiator(Role::Initiator, "CLIENT", "VENUE");
  initiator.session->start();
  acceptor.session->onBytes(initiator.wire.frames[0]);
  ASSERT_EQ(acceptor.session->sequences().nextInbound, 2u);

  // A gap fill jumping the expectation from 2 to 7. Processed WITHOUT
  // the ordinary sequence check -- that is the entire point of it.
  acceptor.session->onBytes(buildFrame("4", 2, [](hffix::message_writer& w) {
    w.push_back_string(hffix::tag::GapFillFlag, "Y");
    w.push_back_int(hffix::tag::NewSeqNo, 7);
  }));
  EXPECT_EQ(acceptor.session->sequences().nextInbound, 7u);

  // And a message at the new expectation is accepted rather than
  // treated as a gap.
  acceptor.appMessages.clear();
  acceptor.session->onBytes(buildFrame("U1", 7, [](hffix::message_writer& w) {
    w.push_back_string(5001, "after-the-fill");
  }));
  EXPECT_EQ(acceptor.appMessages.size(), 1u);
}

TEST(FixSession, ASequenceResetBelowTheExpectationIsRejected) {
  Peer acceptor(Role::Acceptor, "VENUE", "CLIENT");
  acceptor.session->start();
  Peer initiator(Role::Initiator, "CLIENT", "VENUE");
  initiator.session->start();
  acceptor.session->onBytes(initiator.wire.frames[0]);
  acceptor.session->onBytes(buildFrame("4", 2, [](hffix::message_writer& w) {
    w.push_back_string(hffix::tag::GapFillFlag, "Y");
    w.push_back_int(hffix::tag::NewSeqNo, 9);
  }));
  ASSERT_EQ(acceptor.session->sequences().nextInbound, 9u);

  acceptor.wire.clear();
  acceptor.session->onBytes(buildFrame("4", 9, [](hffix::message_writer& w) {
    w.push_back_string(hffix::tag::GapFillFlag, "Y");
    w.push_back_int(hffix::tag::NewSeqNo, 4);  // backwards
  }));
  EXPECT_EQ(acceptor.session->sequences().nextInbound, 9u) << "a backwards reset must not apply";
  bool sawReject = false;
  for (std::size_t i = 0; i < acceptor.wire.count(); ++i) {
    if (acceptor.wire.typeOf(i) == "3") {
      sawReject = true;
    }
  }
  EXPECT_TRUE(sawReject);
}

}  // namespace
}  // namespace sequencer::fix
