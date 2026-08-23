# examples/counter/ — specification.md §10: the one example in this
# repository. At this phase: the state machine, node_main (§15 item 3),
# and replay_main (§15 item 4). The codecs, gateway mains, and load
# generator land in later phases.

add_library(counter_state_machine STATIC
  counter_state_machine.cpp
)
add_library(counter::state_machine ALIAS counter_state_machine)

target_include_directories(counter_state_machine PUBLIC
  "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>"
)
target_link_libraries(counter_state_machine PUBLIC sequencer::node)
target_compile_features(counter_state_machine PUBLIC cxx_std_20)

add_executable(counter_node_main node_main.cpp)
target_link_libraries(counter_node_main PRIVATE counter::state_machine sequencer::node)
set_target_properties(counter_node_main PROPERTIES OUTPUT_NAME counter_node)

add_executable(counter_replay_main replay_main.cpp)
target_link_libraries(counter_replay_main PRIVATE counter::state_machine sequencer::replay)
set_target_properties(counter_replay_main PROPERTIES OUTPUT_NAME counter_replay)

if(SEQUENCER_BUILD_TESTS)
  add_subdirectory(tests)
endif()
