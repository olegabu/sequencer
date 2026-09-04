find_package(Threads REQUIRED)
include(GoogleTest)

function(sequencer_node_test name)
  add_executable(${name} ${name}.cpp)
  target_link_libraries(${name} PRIVATE sequencer::node sequencer::common GTest::gtest GTest::gtest_main Threads::Threads)
  gtest_discover_tests(${name})
endfunction()

sequencer_node_test(committed_entry_ring_test)
sequencer_node_test(apply_loop_test)
sequencer_node_test(node_integration_test)
