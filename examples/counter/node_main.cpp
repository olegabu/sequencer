// The counter example's whole node executable — specification.md §9:
// "an application's whole node executable" is exactly this.

#include <memory>

#include <sequencer/node.hpp>

#include "counter_state_machine.hpp"

int main(int argc, char** argv) {
  return sequencer::RunNode(argc, argv,
                             std::make_unique<sequencer::examples::counter::CounterStateMachine>());
}
