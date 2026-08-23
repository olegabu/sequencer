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

add_executable(counter_codec_test counter_codec_test.cpp)
target_link_libraries(counter_codec_test PRIVATE counter::codecs GTest::gtest GTest::gtest_main)
gtest_discover_tests(counter_codec_test)

add_executable(websocket_transport_test websocket_transport_test.cpp)
target_link_libraries(websocket_transport_test PRIVATE counter::websocket_transport GTest::gtest GTest::gtest_main Threads::Threads)
gtest_discover_tests(websocket_transport_test)

add_executable(kill_leader_drill_test kill_leader_drill_test.cpp)
target_link_libraries(kill_leader_drill_test PRIVATE sequencer::node GTest::gtest GTest::gtest_main Threads::Threads)
target_compile_definitions(kill_leader_drill_test PRIVATE
  COUNTER_NODE_MAIN_PATH="$<TARGET_FILE:counter_node_main>"
)
add_dependencies(kill_leader_drill_test counter_node_main)
gtest_discover_tests(kill_leader_drill_test PROPERTIES TIMEOUT 120)

add_executable(counter_end_to_end_test end_to_end_test.cpp)
target_link_libraries(counter_end_to_end_test PRIVATE sequencer::gateway_input Boost::beast GTest::gtest GTest::gtest_main Threads::Threads)
# The real binaries this test spawns as three subprocesses — built, not
# stubbed, so the test exercises the whole pipeline's actual argv/gflags
# paths exactly as a deployment does.
target_compile_definitions(counter_end_to_end_test PRIVATE
  COUNTER_NODE_MAIN_PATH="$<TARGET_FILE:counter_node_main>"
  COUNTER_INPUT_GATEWAY_MAIN_PATH="$<TARGET_FILE:counter_input_gateway_main>"
  COUNTER_OUTPUT_GATEWAY_MAIN_PATH="$<TARGET_FILE:counter_output_gateway_main>"
)
add_dependencies(counter_end_to_end_test counter_node_main counter_input_gateway_main counter_output_gateway_main)
gtest_discover_tests(counter_end_to_end_test PROPERTIES TIMEOUT 60)
