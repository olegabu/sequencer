#pragma once

// Where live FIX sessions come from.
//
// Lives in session/ rather than in either transport because BOTH need
// it and neither should depend on the other: FixInputTransport owns the
// sockets and implements this; FixOutputTransport consumes it to
// deliver onto those same sessions. That sharing is the point --
// specification.md §8.12 "Shape" requires an order-entry gateway to run
// ONE session core across its input and output halves, so that a
// client's execution reports, aggressive and passive, arrive on the
// order-entry session that owns them.

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

namespace sequencer::fix {

class FixSession;

class SessionSource {
 public:
  virtual ~SessionSource() = default;

  // The session for a routing id, or nullptr if it has gone away
  // between a caller's lookup and its use.
  virtual FixSession* sessionFor(std::uint64_t sessionId) = 0;

  // Every live session's routing id, for broadcast fan-out.
  virtual std::vector<std::uint64_t> liveSessions() = 0;

  // Invoked when a session subscribes to a topic via MarketDataRequest.
  // The input side sees the request; the output side acts on it.
  using SubscribeFn = std::function<void(std::uint64_t sessionId, const std::string& topic)>;
  virtual void setSubscribeFn(SubscribeFn fn) = 0;
};

}  // namespace sequencer::fix
