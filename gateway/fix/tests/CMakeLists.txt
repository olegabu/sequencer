find_package(Threads REQUIRED)
include(GoogleTest)

add_executable(fix_session_test fix_session_test.cpp)
target_link_libraries(fix_session_test PRIVATE sequencer::fix_session
                                                GTest::gtest GTest::gtest_main Threads::Threads)
gtest_discover_tests(fix_session_test)

add_executable(fix_input_transport_test fix_input_transport_test.cpp)
target_link_libraries(fix_input_transport_test PRIVATE sequencer::fix_input
                                                        GTest::gtest GTest::gtest_main Threads::Threads)
gtest_discover_tests(fix_input_transport_test PROPERTIES TIMEOUT 90)

add_executable(fix_output_transport_test fix_output_transport_test.cpp)
target_link_libraries(fix_output_transport_test PRIVATE sequencer::fix_output sequencer::fix_input
                                                         GTest::gtest GTest::gtest_main Threads::Threads)
gtest_discover_tests(fix_output_transport_test PROPERTIES TIMEOUT 90)

add_executable(fix_session_gateway_test fix_session_gateway_test.cpp)
target_link_libraries(fix_session_gateway_test PRIVATE sequencer::fix_gateway sequencer::node
                                                        GTest::gtest GTest::gtest_main Threads::Threads)
gtest_discover_tests(fix_session_gateway_test PROPERTIES TIMEOUT 120)

# TEST-ONLY, and the build graph is what enforces it: quickfix appears
# on exactly this target and no production binary links it
# (03-instruction-fix-gateway.md step 1). QuickFIX's only role here is
# as the conformance CLIENT our own acceptor must interoperate with --
# see specification.md §8.12 and gateway/fix/README.md for why it is
# not used in the gateway itself.
# The rig-not-the-bottleneck measurement (step 7). Its number is quoted
# in bench/load_generator/README.md and gateway/fix/README.md.
add_executable(fix_loadgen_loopback_test fix_loadgen_loopback_test.cpp)
target_link_libraries(fix_loadgen_loopback_test PRIVATE sequencer::bench_fix_requester
                                                         sequencer::fix_input
                                                         GTest::gtest GTest::gtest_main Threads::Threads)
gtest_discover_tests(fix_loadgen_loopback_test PROPERTIES TIMEOUT 180)

find_package(quickfix CONFIG QUIET)
if(quickfix_FOUND)
  add_executable(quickfix_conformance_test quickfix_conformance_test.cpp)
  # Both gateways: the suite runs every test against each, so a real
  # engine judges them on the same terms (specification.md §8.13).
  target_link_libraries(quickfix_conformance_test PRIVATE sequencer::fix_input
                                                           sequencer::quickfix_input quickfix
                                                           GTest::gtest GTest::gtest_main Threads::Threads)
  gtest_discover_tests(quickfix_conformance_test PROPERTIES TIMEOUT 180)
else()
  message(STATUS "quickfix not found; conformance suite skipped")
endif()
