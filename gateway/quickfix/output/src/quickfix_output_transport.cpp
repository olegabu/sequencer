#include <sequencer/quickfix/quickfix_output_transport.hpp>

#include <vector>

namespace sequencer::quickfix {

QuickFixOutputTransport::QuickFixOutputTransport(QuickFixInputTransport& sessions)
    : sessions_(sessions) {
  sessions_.setSubscribeFn([this](std::uint64_t sessionId, const std::string& topic) {
    std::lock_guard<std::mutex> lock(mutex_);
    topicSubscribers_[topic].insert(sessionId);
    if (topics_ != nullptr) {
      topicNames_[topics_->idFor(topic)] = topic;
    }
  });
}

QuickFixOutputTransport::~QuickFixOutputTransport() { stop(); }

void QuickFixOutputTransport::attach(sequencer::BroadcastRing& ring,
                                      sequencer::TopicRegistry& topics, int idleSpinIterations) {
  ring_ = &ring;
  topics_ = &topics;
  idleSpinIterations_ = idleSpinIterations;
}

void QuickFixOutputTransport::start(int /*listenPort*/) {
  // No acceptor of its own: the sockets belong to the input half, which
  // is what makes this one session core rather than two (§8.12
  // "Shape"), and is true whether that core is ours or QuickFIX's.
  if (ring_ == nullptr) {
    return;
  }
  reader_ = std::thread([this] { readLoop(); });
}

void QuickFixOutputTransport::stop() {
  stopping_.store(true, std::memory_order_relaxed);
  if (reader_.joinable()) {
    reader_.join();
  }
}

void QuickFixOutputTransport::readLoop() {
  // ONE reader for every session, as gateway/fix/ uses, and for the
  // same reason: §8.11 requires a session's outputs to arrive in
  // journal order, and one reader walking the ring once gives that by
  // construction.
  std::uint64_t cursor = ring_->head();
  sequencer::IdleStrategy idle(idleSpinIterations_);
  std::vector<std::byte> payload(4096);

  while (!stopping_.load(std::memory_order_relaxed)) {
    std::uint64_t tag = 0;
    std::uint32_t length = 0;
    sequencer::RecordOrigin origin{};
    const auto status = ring_->readOne(cursor, tag, payload.data(), length, origin);
    if (status == sequencer::BroadcastRing::ReadResult::Empty) {
      idle.idle();
      continue;
    }
    if (status == sequencer::BroadcastRing::ReadResult::Overrun) {
      // Lapped. Unlike a per-subscriber transport there is no single
      // client to disconnect, so resume at the head and let the
      // affected sessions recover the hole through QuickFIX's own
      // resend -- which the journal-backed store can serve, because the
      // records are still there.
      cursor = ring_->head();
      continue;
    }
    idle.reset();

    const std::string_view body(reinterpret_cast<const char*>(payload.data()), length);
    // The codec puts MsgType at the front of what it builds, since only
    // the application knows what kind of message it is.
    std::string_view msgType = "U2";
    std::string_view rest = body;
    if (body.size() > 3 && body.compare(0, 3, "35=") == 0) {
      const std::size_t soh = body.find('\001');
      if (soh != std::string_view::npos) {
        msgType = body.substr(3, soh - 3);
        rest = body.substr(soh + 1);
      }
    }

    if ((tag & sequencer::kSessionTagBit) != 0) {
      sessions_.sendApplication(tag & ~sequencer::kSessionTagBit, msgType, rest,
                                 origin.journalSequenceNumber, origin.outputIndex);
      continue;
    }

    std::string topic;
    std::set<std::uint64_t> subscribers;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      const auto named = topicNames_.find(static_cast<std::uint32_t>(tag));
      if (named == topicNames_.end()) {
        continue;
      }
      topic = named->second;
      const auto found = topicSubscribers_.find(topic);
      if (found == topicSubscribers_.end()) {
        continue;
      }
      subscribers = found->second;
    }
    for (const std::uint64_t sessionId : subscribers) {
      sessions_.sendApplication(sessionId, msgType, rest, origin.journalSequenceNumber,
                                 origin.outputIndex);
    }
  }
}

}  // namespace sequencer::quickfix
