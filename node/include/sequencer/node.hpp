#pragma once

// specification.md §4, §5: the whole node executable.

#include <memory>

#include <sequencer/state_machine.hpp>

namespace sequencer {

// Parses its own configuration from argv (raft group and peer ids, data
// directory, election timeout — see node/README.md for the full flag
// list) and runs until asked to stop (SIGINT/SIGTERM). An application's
// entire node binary is:
//
//   int main(int argc, char** argv) {
//     return sequencer::RunNode(argc, argv, std::make_unique<MyStateMachine>());
//   }
int RunNode(int argc, char** argv, std::unique_ptr<StateMachine> stateMachine);

}  // namespace sequencer
