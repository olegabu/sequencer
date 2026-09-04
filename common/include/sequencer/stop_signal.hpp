#pragma once

// "Run until SIGINT or SIGTERM", which six long-running binaries were
// each implementing themselves: the node, and the input, output, relay,
// FIX and QuickFIX gateways. Every copy was the same three pieces -- a
// namespace-scope atomic, a handler that sets it, and a 200ms poll loop
// -- so the only thing the duplication bought was six chances to get
// the reset semantics below wrong.
//
// The flag is a single process-wide inline variable rather than an
// object, because that is what a signal handler can reach: a handler
// takes only an int and so cannot carry any context. `inline` gives it
// one definition across every translation unit that includes this,
// which is exactly the mechanism a plain namespace-scope global does
// NOT have -- and duplicate program-level symbols are a hazard this
// repository has already paid for twice (see gateway/output's
// CMakeLists.txt on gflags).

#include <atomic>
#include <chrono>
#include <csignal>
#include <thread>

namespace sequencer {

inline std::atomic<bool> gStopRequested{false};

inline void handleStopSignal(int /*signum*/) {
  gStopRequested.store(true, std::memory_order_relaxed);
}

inline bool stopRequested() { return gStopRequested.load(std::memory_order_relaxed); }

// Call at the START of a Run* function, before anything is started.
//
// The flag outlives a single run, because it has to be process-wide,
// and a Run* function may be called more than once in one process --
// which the tests do routinely. Without this, a second gateway starting
// after the first was signalled would inherit that SIGTERM and exit
// before it had begun, leaving its threads joinable and taking the
// process down with "terminate called without an active exception".
inline void clearStopRequest() { gStopRequested.store(false, std::memory_order_relaxed); }

// Installs the handlers and blocks until one fires.
//
// Polling rather than sigwait/pause because callers want a plain
// blocking call they can put at the end of main() and because 200ms of
// shutdown latency is irrelevant next to the work of stopping.
inline void waitForStopSignal(std::chrono::milliseconds poll = std::chrono::milliseconds(200)) {
  std::signal(SIGINT, handleStopSignal);
  std::signal(SIGTERM, handleStopSignal);
  while (!stopRequested()) {
    std::this_thread::sleep_for(poll);
  }
}

}  // namespace sequencer
