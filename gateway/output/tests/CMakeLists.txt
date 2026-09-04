find_package(Threads REQUIRED)
include(GoogleTest)

add_executable(broadcast_ring_test broadcast_ring_test.cpp)
target_link_libraries(broadcast_ring_test PRIVATE sequencer::gateway_output GTest::gtest GTest::gtest_main Threads::Threads)
gtest_discover_tests(broadcast_ring_test)

add_executable(output_gateway_test output_gateway_test.cpp)
# Links the WebSocket transport too: OneGatewayServesTwoTransportsFromOneJournalTail
# drives two different protocols from a single gateway, which is the
# property the multi-transport chassis exists for.
target_link_libraries(output_gateway_test PRIVATE sequencer::gateway_output sequencer::gateway_output_websocket GTest::gtest GTest::gtest_main Threads::Threads sequencer::common)
gtest_discover_tests(output_gateway_test)

add_executable(output_gateway_restart_drill_test restart_drill_test.cpp)
target_link_libraries(output_gateway_restart_drill_test PRIVATE sequencer::gateway_output GTest::gtest GTest::gtest_main Threads::Threads sequencer::common)
gtest_discover_tests(output_gateway_restart_drill_test PROPERTIES TIMEOUT 60)

add_executable(brpc_output_transport_test brpc_output_transport_test.cpp)
target_link_libraries(brpc_output_transport_test PRIVATE sequencer::gateway_output GTest::gtest GTest::gtest_main Threads::Threads)
gtest_discover_tests(brpc_output_transport_test)

add_executable(websocket_output_transport_test websocket_output_transport_test.cpp)
target_link_libraries(websocket_output_transport_test PRIVATE sequencer::gateway_output_websocket GTest::gtest GTest::gtest_main Threads::Threads)
gtest_discover_tests(websocket_output_transport_test)

add_executable(grpc_output_transport_test grpc_output_transport_test.cpp)
target_link_libraries(grpc_output_transport_test PRIVATE sequencer::gateway_output_grpc gRPC::grpc++ GTest::gtest GTest::gtest_main Threads::Threads)
gtest_discover_tests(grpc_output_transport_test)
