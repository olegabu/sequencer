#pragma once

// Delivery onto QuickFIX sessions (specification.md §8.13).
//
// The counterpart to gateway/fix/'s FixOutputTransport, and much
// smaller, because the things that made that one large are QuickFIX's
// here:
//
//   - no resend source. A ResendRequest is answered by QuickFIX out of
//     the journal-backed message store, so this class never sees one.
//   - no catch-up. When a session logs on, QuickFIX compares sequence
//     numbers and asks for what it missed; the store serves it. The
//     hffix gateway has to replay the journal itself because nothing
//     else would.
//   - no delivery dedup. There is one path here. The hffix gateway
//     needs a high-water mark because catch-up and the live reader can
//     both see a record.
//
// What remains is the part that is genuinely ours: read the ring in
// journal order, and hand each output to the session it addresses.

#include <sequencer/broadcast_ring.hpp>
#include <sequencer/output_transport.hpp>
#include <sequencer/quickfix/quickfix_input_transport.hpp>

#include <atomic>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>

namespace sequencer::quickfix {

class QuickFixOutputTransport : public sequencer::OutputTransport {
 public:
  explicit QuickFixOutputTransport(QuickFixInputTransport& sessions);
  ~QuickFixOutputTransport() override;

  void attach(sequencer::BroadcastRing& ring, sequencer::TopicRegistry& topics,
              int idleSpinIterations) override;
  void start(int listenPort) override;
  void stop() override;

 private:
  void readLoop();

  QuickFixInputTransport& sessions_;
  sequencer::BroadcastRing* ring_ = nullptr;
  sequencer::TopicRegistry* topics_ = nullptr;
  int idleSpinIterations_ = 1000;

  std::mutex mutex_;
  std::map<std::string, std::set<std::uint64_t>> topicSubscribers_;
  std::map<std::uint32_t, std::string> topicNames_;

  std::thread reader_;
  std::atomic<bool> stopping_{false};
};

}  // namespace sequencer::quickfix
