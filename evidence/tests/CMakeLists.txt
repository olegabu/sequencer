find_package(Threads REQUIRED)
include(GoogleTest)

add_executable(evidence_block_test block_test.cpp)
target_link_libraries(evidence_block_test PRIVATE sequencer::evidence GTest::gtest GTest::gtest_main)
gtest_discover_tests(evidence_block_test)

add_executable(evidence_merkle_test merkle_test.cpp)
target_link_libraries(evidence_merkle_test PRIVATE sequencer::evidence GTest::gtest GTest::gtest_main)
gtest_discover_tests(evidence_merkle_test)

add_executable(evidence_signing_gateway_test signing_gateway_test.cpp)
target_link_libraries(evidence_signing_gateway_test PRIVATE
  sequencer::evidence_proto
  unofficial::brpc::brpc-static
  GTest::gtest
  GTest::gtest_main
  Threads::Threads
)
gtest_discover_tests(evidence_signing_gateway_test PROPERTIES TIMEOUT 60)
