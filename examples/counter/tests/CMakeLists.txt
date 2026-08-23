find_package(Threads REQUIRED)
include(GoogleTest)

add_executable(counter_state_machine_test counter_state_machine_test.cpp)
target_link_libraries(counter_state_machine_test PRIVATE counter::state_machine GTest::gtest GTest::gtest_main)
gtest_discover_tests(counter_state_machine_test)

add_executable(three_node_smoke_test three_node_smoke_test.cpp)
target_link_libraries(three_node_smoke_test PRIVATE sequencer::node GTest::gtest GTest::gtest_main Threads::Threads)
# The real counter_node binary this test spawns as three subprocesses —
# built, not stubbed, so the test exercises RunNode's actual argv/gflags
# path exactly as a deployment does.
target_compile_definitions(three_node_smoke_test PRIVATE
  COUNTER_NODE_MAIN_PATH="$<TARGET_FILE:counter_node_main>"
)
add_dependencies(three_node_smoke_test counter_node_main)
gtest_discover_tests(three_node_smoke_test PROPERTIES TIMEOUT 60)

add_executable(counter_replay_test replay_test.cpp)
target_link_libraries(counter_replay_test PRIVATE counter::state_machine sequencer::replay GTest::gtest GTest::gtest_main)
gtest_discover_tests(counter_replay_test)
