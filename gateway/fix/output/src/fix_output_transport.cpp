#include <sequencer/fix/fix_output_transport.hpp>

#include <sequencer/journal/reader.hpp>

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
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
// Splits the codec's "35=<type>\001<fields...>" body into the MsgType
// and the rest.
//
// The codec puts MsgType at the front of what it builds, because only
// the application knows whether a message is an execution report, a
// market-data snapshot or something else. The session core needs it
// separately, to place it immediately after BodyLength where FIX
// requires rather than in the middle of the body.
//
// One function because all three delivery routes -- live ring delivery,
// catch-up, and resend -- have to split it IDENTICALLY. They were three
// copies of the same six lines, and a resend that split differently
// from the original send would put a different message on the wire than
// the one being resent.
struct SplitBody {
  std::string_view msgType;
  std::string_view rest;
};

inline SplitBody splitMsgType(std::string_view body, std::string_view fallbackType) {
  if (body.size() > 3 && body.compare(0, 3, "35=") == 0) {
    const std::size_t soh = body.find('\001');
    if (soh != std::string_view::npos) {
      return {body.substr(3, soh - 3), body.substr(soh + 1)};
    }
  }
  return {fallbackType, body};
}

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

      // Falls back to the MsgType recorded when this message was
      // originally sent, so a resend names the same type even if the
      // codec's output no longer carries one.
      const auto [msgType, rest] = splitMsgType(raw, record->msgType);
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

  // Per-session high-water mark of what has actually gone out, as
  // (journal record, output index). In memory only: it exists to settle
  // the narrow catch-up/live-reader race within one process. Across a
  // restart the persisted lastJournalSequence is what matters, and
  // catch-up resumes from there.
  bool alreadyDelivered(const std::string& sessionKey, const sequencer::RecordOrigin& origin) {
    std::lock_guard<std::mutex> lock(highWaterMutex);
    return alreadyDeliveredLocked(sessionKey, origin);
  }

  // Check and mark as ONE atomic step, returning true to the single
  // caller that won the right to send this output.
  //
  // Testing alreadyDelivered() and then calling markDelivered() after
  // the send is a check-then-act race: two threads both pass the test
  // before either marks, and the client gets the message twice. That
  // was always reachable between catch-up and the live ring reader --
  // the window the comment in deliver() describes -- but rare enough
  // never to be seen. Answering inline (deliverInline) puts a second
  // thread on this path for EVERY message and it reproduced in 1 run
  // in 6.
  //
  // Marking before the send means a send that throws still counts as
  // delivered. That is the right way round: a lost message is
  // recoverable by ResendRequest, a duplicate MsgSeqNum is not.
  bool claimDelivery(const std::string& sessionKey, const sequencer::RecordOrigin& origin) {
    std::lock_guard<std::mutex> lock(highWaterMutex);
    if (alreadyDeliveredLocked(sessionKey, origin)) {
      return false;
    }
    markDeliveredLocked(sessionKey, origin);
    return true;
  }

  // One output's identity, packed so a set can hold it cheaply.
  static std::uint64_t packOrigin(const sequencer::RecordOrigin& origin) {
    return (origin.journalSequenceNumber << 16) | (origin.outputIndex & 0xFFFFu);
  }

  // MEMBERSHIP, not a high-water mark.
  //
  // A mark alone is correct only if outputs are offered in order, which
  // is true of the journal path and false of the inline one: propose
  // callbacks complete out of order, so record 101 could claim before
  // record 100. With a mark, 101 raised it and then BOTH of record
  // 100's copies -- inline and journal -- tested "below the mark" and
  // were suppressed, so that reply was never sent at all. At 20k that
  // lost 2 replies per run and the rig sat out its whole drain timeout
  // waiting for them; at 100k out-of-order completion is the norm and
  // it lost most of them.
  //
  // `mark` survives only as a pruning floor: everything at or below it
  // is assumed delivered, which lets `claimed` stay bounded.
  bool alreadyDeliveredLocked(const std::string& sessionKey,
                               const sequencer::RecordOrigin& origin) {
    const auto it = claims.find(sessionKey);
    if (it == claims.end()) {
      return false;
    }
    const std::uint64_t packed = packOrigin(origin);
    return packed <= it->second.mark || it->second.claimed.count(packed) != 0;
  }

  void markDeliveredLocked(const std::string& sessionKey,
                            const sequencer::RecordOrigin& origin) {
    Claims& c = claims[sessionKey];
    const std::uint64_t packed = packOrigin(origin);
    if (packed <= c.mark) {
      return;
    }
    c.claimed.insert(packed);
    // Bounded: the two paths are only ever a few hundred microseconds
    // apart, so the live set is tiny. The window is orders of magnitude
    // larger than that, and dropping the smallest entry into the mark
    // is safe because it HAS been delivered.
    while (c.claimed.size() > kClaimWindow) {
      const auto oldest = c.claimed.begin();
      c.mark = *oldest;
      c.claimed.erase(oldest);
    }
  }

  void markDelivered(const std::string& sessionKey, const sequencer::RecordOrigin& origin) {
    std::lock_guard<std::mutex> lock(highWaterMutex);
    markDeliveredLocked(sessionKey, origin);
  }

  std::mutex highWaterMutex;
  struct Claims {
    std::uint64_t mark = 0;             // everything at or below is delivered
    std::set<std::uint64_t> claimed;    // delivered outputs above the mark
  };
  static constexpr std::size_t kClaimWindow = 65536;
  std::map<std::string, Claims> claims;

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
    const auto [msgType, rest] = splitMsgType(body, "U2");
    // Skip what catch-up already sent. Both paths can see the same
    // record in the narrow window between catch-up reading the journal's
    // committed count and the session becoming live on the ring, and a
    // duplicate execution report is worse than a late one.
    //
    // Compared as (record, output index), NOT record alone: ONE record
    // may emit SEVERAL outputs -- an aggressive fill and the passive
    // fill it caused, in the order-entry case -- and a record-only
    // comparison drops every output after the first, which is exactly
    // what it did when first written.
    if (origin.journalSequenceNumber != 0 &&
        !claimDelivery(session->sessionKey(), origin)) {
      return;
    }

    const std::uint64_t outboundSeqNum = session->sendApplication(msgType, rest);
    if (origin.journalSequenceNumber != 0) {
      session->setLastJournalSequence(origin.journalSequenceNumber);
    }

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
  impl_->sessions.setSubscribeFn([this](std::uint64_t sessionId, const std::string& topic,
                                        SessionSource::SubscriptionAction action) {
    // Snapshot (263=0) deliberately does nothing here. It is not a
    // subscription, and the request has already gone to the codec,
    // which is the layer that can answer it: the application holds the
    // state, the transport holds only the routing.
    if (action == SessionSource::SubscriptionAction::Snapshot) {
      return;
    }
    std::lock_guard<std::mutex> lock(impl_->subscriptionsMutex);
    if (action == SessionSource::SubscriptionAction::Unsubscribe) {
      const auto it = impl_->topicSubscribers.find(topic);
      if (it != impl_->topicSubscribers.end()) {
        it->second.erase(sessionId);
        if (it->second.empty()) {
          impl_->topicSubscribers.erase(it);
        }
      }
      return;
    }
    impl_->topicSubscribers[topic].insert(sessionId);
    if (impl_->topics != nullptr) {
      impl_->topicNames[impl_->topics->idFor(topic)] = topic;
    }
  });
}

FixOutputTransport::~FixOutputTransport() { stop(); }

void FixOutputTransport::catchUp(FixSession& session) {
  if (impl_->journal == nullptr || impl_->codec == nullptr) {
    return;
  }
  const std::uint64_t from = session.sequences().lastJournalSequence + 1;
  const std::uint64_t through = impl_->journal->committedCount();
  if (from > through) {
    return;  // nothing missed
  }

  // Bounded on purpose: a session away for a long time could otherwise
  // block its own Logon while the whole journal is replayed onto it.
  // Beyond this the client is told nothing special -- it simply starts
  // from a later point, which is the same guarantee a FIX session gives
  // after an operator-forced reset. A deployment that needs full
  // history points the client at the relay (§8.2), which exists for
  // exactly that and serves it without competing with live delivery.
  constexpr std::uint64_t kMaxCatchUpRecords = 10000;
  const std::uint64_t start =
      (through - from + 1) > kMaxCatchUpRecords ? (through - kMaxCatchUpRecords + 1) : from;

  for (std::uint64_t seq = start; seq <= through; ++seq) {
    if (!impl_->journal->contains(seq)) {
      continue;
    }
    CapturingFanout captured;
    impl_->codec->toOutput(impl_->journal->record(seq), captured);
    std::uint32_t outputIndex = 0;
    for (const sequencer::Bytes& body : captured.outputs) {
      const std::string_view raw(reinterpret_cast<const char*>(body.data()), body.size());
      const auto [msgType, rest] = splitMsgType(raw, "U2");
      const std::uint64_t outboundSeqNum = session.sendApplication(msgType, rest);
      SentRecord record;
      record.journalSequenceNumber = seq;
      record.outputIndex = outputIndex;
      record.msgType = std::string(msgType);
      impl_->recordSent(session.sessionKey(), outboundSeqNum, record);
      impl_->markDelivered(session.sessionKey(),
                            sequencer::RecordOrigin{seq, outputIndex});
      ++outputIndex;
    }
    session.setLastJournalSequence(seq);
  }
}

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
    // Catch the session up on what it missed while away, BEFORE it can
    // be sent anything live. specification.md §8.12: re-read the journal
    // from the session's last persisted position. These go out as NEW
    // messages, not resends -- the gateway never sent them, so there is
    // no sequence number to replay (see SequenceNumbers::
    // lastJournalSequence for why ResendRequest cannot do this).
    catchUp(session);

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

    // Drains a BATCH of ring entries, then flushes each session that
    // was written to exactly once. One socket write carries however
    // many messages were already waiting -- see SessionSource's
    // beginBatch() for why that is safe and why it costs an idle
    // session nothing.
    constexpr int kMaxDrain = 1024;
    std::vector<std::uint64_t> touched;

    // Stage timers. The question they answer is the one a profile could
    // not: is this reader STARVED -- waiting on entries that arrive too
    // slowly -- or BUSY, spending real time delivering each one? Those
    // point at opposite halves of the system and the fix differs
    // completely, so guessing between them is what these exist to
    // avoid.
    //
    // Reported to the log every kReportIntervalNs rather than through
    // bvar, because this gateway runs no brpc server and so has no
    // /vars page to read them from.
    using Ns = std::chrono::nanoseconds;
    // Off unless asked for -- and off means NO clock read at all, not
    // just no printing.
    //
    // This loop turns ~3.9M drains/sec. Reading the clock ~5 times per
    // iteration regardless of the switch cost ~19M clock_gettime/sec,
    // which a profile put at 47.8% of the gateway's CPU -- the timers
    // were the single largest consumer in the thing they were added to
    // measure, and they inflated the very idle/wait percentages they
    // reported. Every now() below is now behind `timed`.
    const bool timed = std::getenv("FIX_STAGE_TIMERS") != nullptr;
    const auto now = [] { return std::chrono::steady_clock::now(); };
    const auto nowIfTimed = [&timed, &now] {
      return timed ? now() : std::chrono::steady_clock::time_point{};
    };
    std::uint64_t idleNs = 0, deliverNs = 0, flushNs = 0;
    std::uint64_t entries = 0, drains = 0, emptyDrains = 0;
    // How long an entry sat in the ring before this reader picked it
    // up. This is the hop between the journal and the wire that no
    // other counter covers -- if it is large, the reader is not
    // noticing work promptly; if it is small, the latency is upstream.
    std::uint64_t ringWaitUs = 0;
    auto lastReport = now();
    constexpr std::uint64_t kReportIntervalNs = 2'000'000'000ULL;
      // Off unless asked for: these are diagnostics, and a sweep
      // does not want them in its logs. Set FIX_STAGE_TIMERS=1.
      const bool stageTimers = std::getenv("FIX_STAGE_TIMERS") != nullptr;

    while (!impl_->stopping.load(std::memory_order_relaxed)) {
      touched.clear();
      int gathered = 0;
      bool sawOverrun = false;

      while (gathered < kMaxDrain) {
        std::uint64_t tag = 0;
        std::uint32_t length = 0;
        sequencer::RecordOrigin origin;
        const auto result =
            impl_->ring->readOne(cursor, tag, payload.data(), length, origin);
        if (result == sequencer::BroadcastRing::ReadResult::Empty) {
          break;
        }
        if (result == sequencer::BroadcastRing::ReadResult::Overrun) {
          sawOverrun = true;
          break;
        }

        const std::string_view body(reinterpret_cast<const char*>(payload.data()), length);
        auto deliverTo = [&](std::uint64_t sessionId) {
          if (std::find(touched.begin(), touched.end(), sessionId) == touched.end()) {
            impl_->sessions.beginBatch(sessionId);
            touched.push_back(sessionId);
          }
          impl_->deliver(sessionId, body, origin);
        };

        if (timed && origin.publishTimeUs != 0) {
          const auto nowUs = static_cast<std::uint64_t>(
              std::chrono::duration_cast<std::chrono::microseconds>(
                  std::chrono::steady_clock::now().time_since_epoch())
                  .count());
          ringWaitUs += (nowUs > origin.publishTimeUs) ? (nowUs - origin.publishTimeUs) : 0;
        }
        const auto deliverStart = nowIfTimed();
        if ((tag & sequencer::kSessionTagBit) != 0) {
          deliverTo(tag & ~sequencer::kSessionTagBit);
        } else {
          const std::string topic = impl_->topicNameFor(static_cast<std::uint32_t>(tag));
          if (!topic.empty()) {
            for (const std::uint64_t sessionId : impl_->subscribersOf(topic)) {
              deliverTo(sessionId);
            }
          }
        }
        if (timed) {
          deliverNs += static_cast<std::uint64_t>(
              std::chrono::duration_cast<Ns>(now() - deliverStart).count());
        }
        ++gathered;
        ++entries;
      }

      // One syscall per session per drain.
      const auto flushStart = nowIfTimed();
      for (const std::uint64_t sessionId : touched) {
        impl_->sessions.endBatch(sessionId);
      }
      if (timed) {
        flushNs += static_cast<std::uint64_t>(
            std::chrono::duration_cast<Ns>(now() - flushStart).count());
      }
      ++drains;

      if (sawOverrun) {
        // Lapped by the producer. Unlike a per-subscriber transport
        // there is no single client to disconnect, so resume from the
        // current head and let the affected sessions recover the hole
        // from the journal -- which is what §8.12 reason 1's
        // journal-as-resend-store makes possible.
        cursor = impl_->ring->head();
        continue;
      }
      if (gathered == 0) {
        ++emptyDrains;
        const auto idleStart = nowIfTimed();
        idle.idle();
        if (timed) {
          idleNs += static_cast<std::uint64_t>(
              std::chrono::duration_cast<Ns>(now() - idleStart).count());
        }
      } else {
        idle.reset();
      }

      // The report cadence check was itself a per-iteration clock read.
      if (!timed) {
        continue;
      }
      const auto sinceReport = static_cast<std::uint64_t>(
          std::chrono::duration_cast<Ns>(now() - lastReport).count());
      if (stageTimers && sinceReport >= kReportIntervalNs &&
          (entries > 0 || emptyDrains > 0)) {
        // STARVED vs BUSY reads straight off these: idle dominating the
        // window means entries are not arriving; deliver or flush
        // dominating means each one costs too much.
        std::fprintf(stderr,
                     "[fix-out] window=%.2fs entries=%llu drains=%llu empty=%llu "
                     "idle=%.1f%% deliver=%.1f%% flush=%.1f%% "
                     "deliver_per_entry=%.1fus flush_per_drain=%.1fus "
                     "ring_wait_per_entry=%.1fus\n",
                     static_cast<double>(sinceReport) / 1e9,
                     (unsigned long long)entries, (unsigned long long)drains,
                     (unsigned long long)emptyDrains,
                     100.0 * static_cast<double>(idleNs) / static_cast<double>(sinceReport),
                     100.0 * static_cast<double>(deliverNs) / static_cast<double>(sinceReport),
                     100.0 * static_cast<double>(flushNs) / static_cast<double>(sinceReport),
                     entries ? static_cast<double>(deliverNs) / entries / 1000.0 : 0.0,
                     drains ? static_cast<double>(flushNs) / drains / 1000.0 : 0.0,
                     entries ? static_cast<double>(ringWaitUs) / entries : 0.0);
        idleNs = deliverNs = flushNs = 0;
        entries = drains = emptyDrains = 0;
        ringWaitUs = 0;
        lastReport = now();
      }
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

void FixOutputTransport::deliverInline(std::uint64_t sessionId, std::string_view body,
                                        std::uint64_t journalSequenceNumber,
                                        std::uint32_t lastOutputIndex) {
  // The session must be logged on: an inline answer to an order that
  // arrived before Logon completed would jump the session's own
  // handshake. deliver() also tolerates a session that vanished.
  //
  // Batching is deliberately not used here. One request produces one
  // message and the client is waiting for it -- coalescing would trade
  // the very latency this path exists to save.
  impl_->deliver(sessionId, body,
                 sequencer::RecordOrigin{journalSequenceNumber, lastOutputIndex, 0});
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
