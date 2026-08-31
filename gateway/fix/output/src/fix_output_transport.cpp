#include <sequencer/fix/fix_output_transport.hpp>

#include <sequencer/journal/reader.hpp>

#include <atomic>
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace sequencer::fix {

// Collects what a codec publishes for one record, so a resend can pick
// out the single output it needs by index.
class CapturingFanout final : public sequencer::Fanout {
 public:
  void toSession(sequencer::SessionId, sequencer::Bytes bytes) override {
    outputs.push_back(std::move(bytes));
  }
  void broadcast(const std::string&, sequencer::Bytes bytes) override {
    outputs.push_back(std::move(bytes));
  }
  std::vector<sequencer::Bytes> outputs;
};

// Serves a ResendRequest for one session by re-reading the journal.
class JournalResendSource final : public ResendSource {
 public:
  JournalResendSource(FixOutputTransport& transport, std::string sessionKey,
                       sequencer::journal::JournalReader& reader, sequencer::OutputCodec& codec)
      : transport_(transport), sessionKey_(std::move(sessionKey)), reader_(reader), codec_(codec) {}

  bool resend(std::uint64_t begin, std::uint64_t end, const Emit& emit) override {
    bool servedAny = false;
    for (std::uint64_t seqNum = begin; seqNum <= end; ++seqNum) {
      const SentRecord* record = transport_.sentRecord(sessionKey_, seqNum);
      if (record == nullptr || record->journalSequenceNumber == 0) {
        // Nothing was sent at this outbound number, or it was an
        // administrative message -- neither is resent, and the session
        // core gap-fills the hole.
        continue;
      }
      if (!reader_.contains(record->journalSequenceNumber)) {
        continue;  // journal no longer holds it
      }

      // The same record through the same codec is the same bytes --
      // which is what makes an outbound message store unnecessary
      // (§8.12 reason 1). This is the one place that determinism is
      // load-bearing outside replay (§11).
      CapturingFanout captured;
      codec_.toOutput(reader_.record(record->journalSequenceNumber), captured);
      if (record->outputIndex >= captured.outputs.size()) {
        continue;
      }
      const sequencer::Bytes& body = captured.outputs[record->outputIndex];
      const std::string_view raw(reinterpret_cast<const char*>(body.data()), body.size());

      // Strip the codec's leading MsgType so the session core can place
      // it where FIX requires -- the same split deliver() does.
      std::string_view msgType = record->msgType;
      std::string_view rest = raw;
      if (raw.size() > 3 && raw.compare(0, 3, "35=") == 0) {
        const std::size_t soh = raw.find('\001');
        if (soh != std::string_view::npos) {
          msgType = raw.substr(3, soh - 3);
          rest = raw.substr(soh + 1);
        }
      }
      emit(seqNum, msgType, rest, record->sendingTime);
      servedAny = true;
    }
    return servedAny;
  }

 private:
  FixOutputTransport& transport_;
  // The CompID pair, which survives a reconnect -- see
  // FixOutputTransport::sentRecord().
  std::string sessionKey_;
  sequencer::journal::JournalReader& reader_;
  sequencer::OutputCodec& codec_;
};

struct FixOutputTransport::Impl {
  explicit Impl(SessionSource& s) : sessions(s) {}

  SessionSource& sessions;
  sequencer::BroadcastRing* ring = nullptr;
  sequencer::TopicRegistry* topics = nullptr;
  int idleSpinIterations = 100;

  std::atomic<bool> stopping{false};
  std::thread reader;

  // Which sessions asked for which topic, by MarketDataRequest. A topic
  // with no subscribers is delivered to nobody -- FIX's own
  // subscription model, which is how §8.10's open topic question is
  // resolved (§8.12 "Shape").
  std::mutex subscriptionsMutex;
  std::map<std::string, std::set<std::uint64_t>> topicSubscribers;
  std::map<std::uint32_t, std::string> topicNames;

  // (session, outbound MsgSeqNum) -> where that message came from in
  // the journal. specification.md §8.12 reason 1: this is what replaces
  // an outbound message store -- a ResendRequest re-reads the journal
  // at these positions rather than replaying a saved copy.
  mutable std::mutex sentMutex;
  std::unordered_map<std::string, std::map<std::uint64_t, SentRecord>> sent;

  // Set by attachJournal(): what a resend re-reads.
  std::unique_ptr<sequencer::journal::JournalReader> journal;
  sequencer::OutputCodec* codec = nullptr;
  std::vector<std::unique_ptr<JournalResendSource>> resendSources;
  std::mutex resendMutex;

  void recordSent(const std::string& sessionKey, std::uint64_t seqNum,
                   const SentRecord& record) {
    std::lock_guard<std::mutex> lock(sentMutex);
    sent[sessionKey][seqNum] = record;
  }

  std::set<std::uint64_t> subscribersOf(const std::string& topic) {
    std::lock_guard<std::mutex> lock(subscriptionsMutex);
    const auto it = topicSubscribers.find(topic);
    return it == topicSubscribers.end() ? std::set<std::uint64_t>{} : it->second;
  }

  std::string topicNameFor(std::uint32_t topicId) {
    std::lock_guard<std::mutex> lock(subscriptionsMutex);
    const auto it = topicNames.find(topicId);
    return it == topicNames.end() ? std::string{} : it->second;
  }

  // Delivers one ring entry as a FIX message on one session. The body
  // is whatever the OutputCodec produced; the session core supplies
  // MsgSeqNum, SendingTime and CheckSum.
  void deliver(std::uint64_t sessionId, std::string_view body,
                const sequencer::RecordOrigin& origin) {
    FixSession* session = sessions.sessionFor(sessionId);
    if (session == nullptr) {
      return;  // gone between the ring read and here
    }
    // The codec puts MsgType at the front of the body it builds, since
    // only the application knows whether this is an execution report,
    // a market-data snapshot, or something else. Split it off so the
    // session core can place it where FIX requires -- immediately after
    // BodyLength -- rather than in the middle of the body.
    std::string_view msgType = "U2";
    std::string_view rest = body;
    if (body.size() > 3 && body.compare(0, 3, "35=") == 0) {
      const std::size_t soh = body.find('\001');
      if (soh != std::string_view::npos) {
        msgType = body.substr(3, soh - 3);
        rest = body.substr(soh + 1);
      }
    }
    const std::uint64_t outboundSeqNum = session->sendApplication(msgType, rest);

    // What a later ResendRequest needs. specification.md §8.12 reason
    // 1: there is no outbound message store -- this records WHERE in
    // the journal the message came from, and a resend re-reads it.
    // That is what lets a resend survive a restart, which a copy held
    // in memory cannot.
    SentRecord record;
    record.journalSequenceNumber = origin.journalSequenceNumber;
    record.outputIndex = origin.outputIndex;
    record.msgType = std::string(msgType);
    // Keyed by the session's FIX identity, so a reconnect finds it.
    recordSent(session->sessionKey(), outboundSeqNum, record);
  }
};

FixOutputTransport::FixOutputTransport(SessionSource& sessions)
    : impl_(std::make_unique<Impl>(sessions)) {
  impl_->sessions.setSubscribeFn([this](std::uint64_t sessionId, const std::string& topic) {
    std::lock_guard<std::mutex> lock(impl_->subscriptionsMutex);
    impl_->topicSubscribers[topic].insert(sessionId);
    if (impl_->topics != nullptr) {
      impl_->topicNames[impl_->topics->idFor(topic)] = topic;
    }
  });
}

FixOutputTransport::~FixOutputTransport() { stop(); }

void FixOutputTransport::attachJournal(const std::filesystem::path& dataDir,
                                        sequencer::OutputCodec& codec) {
  impl_->journal = std::make_unique<sequencer::journal::JournalReader>(dataDir / "journal");
  impl_->codec = &codec;

  // Install a ResendSource on each session AS IT LOGS ON, not lazily on
  // first delivery: a ResendRequest is commonly the first thing a
  // reconnecting client sends, so it must already be servable.
  impl_->sessions.setSessionReadyFn([this](std::uint64_t /*sessionId*/, FixSession& session) {
    if (impl_->journal == nullptr || impl_->codec == nullptr) {
      return;
    }
    // The session's own key, captured now that Logon has adopted the
    // peer's identity -- which is why this runs on session-ready and not
    // at accept time.
    auto source = std::make_unique<JournalResendSource>(*this, session.sessionKey(),
                                                         *impl_->journal, *impl_->codec);
    session.setResendSource(source.get());
    std::lock_guard<std::mutex> lock(impl_->resendMutex);
    impl_->resendSources.push_back(std::move(source));
  });
}

void FixOutputTransport::attach(sequencer::BroadcastRing& ring, sequencer::TopicRegistry& topics,
                                 int idleSpinIterations) {
  impl_->ring = &ring;
  impl_->topics = &topics;
  impl_->idleSpinIterations = idleSpinIterations;
}

void FixOutputTransport::start(int /*listenPort*/) {
  // No acceptor of its own in the order-entry shape: the sockets belong
  // to FixInputTransport, and this side writes onto the very sessions
  // that submitted the orders (§8.12 "Shape"). A market-data-only
  // deployment would own an acceptor here instead.
  if (impl_->ring == nullptr) {
    return;
  }

  // ONE reader for every session, rather than one per session as the
  // WebSocket and gRPC transports use. The reason is §8.11's ordering
  // guarantee: a session's outputs must arrive in journal order, and
  // one reader walking the ring once, dispatching each entry to the
  // session it addresses, gives that by construction. Per-session
  // readers would each hold their own cursor and could interleave a
  // client's fills across different orders, which is exactly the
  // cross-order hazard §8.11 exists to prevent.
  impl_->reader = std::thread([this] {
    std::uint64_t cursor = impl_->ring->head();
    sequencer::IdleStrategy idle(impl_->idleSpinIterations);
    std::vector<std::byte> payload(impl_->ring->maxPayload());

    while (!impl_->stopping.load(std::memory_order_relaxed)) {
      std::uint64_t tag = 0;
      std::uint32_t length = 0;
      sequencer::RecordOrigin origin;
      const auto result =
          impl_->ring->readOne(cursor, tag, payload.data(), length, origin);
      if (result == sequencer::BroadcastRing::ReadResult::Empty) {
        idle.idle();
        continue;
      }
      if (result == sequencer::BroadcastRing::ReadResult::Overrun) {
        // Lapped by the producer. Unlike a per-subscriber transport
        // there is no single client to disconnect, so the honest
        // response is to resume from the current head and let the
        // affected sessions' ResendRequests recover the hole from the
        // journal -- which is precisely what §8.12 reason 1's
        // journal-as-resend-store makes possible.
        cursor = impl_->ring->head();
        continue;
      }

      const std::string_view body(reinterpret_cast<const char*>(payload.data()), length);
      if ((tag & sequencer::kSessionTagBit) != 0) {
        impl_->deliver(tag & ~sequencer::kSessionTagBit, body, origin);
      } else {
        // A broadcast reaches only the sessions that asked for this
        // topic through MarketDataRequest.
        const std::string topic = impl_->topicNameFor(static_cast<std::uint32_t>(tag));
        if (!topic.empty()) {
          for (const std::uint64_t sessionId : impl_->subscribersOf(topic)) {
            impl_->deliver(sessionId, body, origin);
          }
        }
      }
      idle.reset();
    }
  });
}

void FixOutputTransport::stop() {
  if (impl_ == nullptr || impl_->stopping.exchange(true)) {
    return;
  }
  if (impl_->reader.joinable()) {
    impl_->reader.join();
  }
}

const SentRecord* FixOutputTransport::sentRecord(const std::string& sessionKey,
                                                  std::uint64_t outboundSeqNum) const {
  std::lock_guard<std::mutex> lock(impl_->sentMutex);
  const auto session = impl_->sent.find(sessionKey);
  if (session == impl_->sent.end()) {
    return nullptr;
  }
  const auto record = session->second.find(outboundSeqNum);
  return record == session->second.end() ? nullptr : &record->second;
}

std::size_t FixOutputTransport::sentRecordCount(const std::string& sessionKey) const {
  std::lock_guard<std::mutex> lock(impl_->sentMutex);
  const auto session = impl_->sent.find(sessionKey);
  return session == impl_->sent.end() ? 0 : session->second.size();
}

}  // namespace sequencer::fix
