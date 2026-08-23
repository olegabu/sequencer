// specification.md §11: "An application links this [tools/replay] and
// exposes its own replay binary (as the counter example does)."

#include <memory>

#include <sequencer/replay.hpp>

#include "counter_state_machine.hpp"

int main(int argc, char** argv) {
  return sequencer::RunReplayCheck(
      argc, argv, std::make_unique<sequencer::examples::counter::CounterStateMachine>());
}
