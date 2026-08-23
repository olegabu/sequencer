#pragma once

// specification.md §11: determinism certification — the sequencer's
// warranty. Replays a recorded input sequence through a fresh
// StateMachine instance and compares the resulting journal
// byte-for-byte against the original. Because it needs a concrete
// state machine, this is a library, invoked the same way RunNode is;
// an application links it and exposes its own replay binary (as
// examples/counter/replay_main.cpp does).
//
// Any divergence is a bug — in the state machine (a §4.1 rule
// violated), the harness, or the journal protocol itself.

#include <memory>

#include <sequencer/state_machine.hpp>

namespace sequencer {

// Reads the journal at --data_dir, replays every committed input
// through `stateMachine`, and compares the resulting journal
// byte-for-byte against the original. Prints a diagnostic and returns
// nonzero on any divergence, read failure, or empty journal check
// target; returns 0 only if every record matched exactly.
int RunReplayCheck(int argc, char** argv, std::unique_ptr<StateMachine> stateMachine);

}  // namespace sequencer
