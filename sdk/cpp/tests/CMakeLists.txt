find_package(Threads REQUIRED)
include(GoogleTest)

add_executable(sdk_client_signer_test client_signer_test.cpp)
target_link_libraries(sdk_client_signer_test PRIVATE sequencer::sdk GTest::gtest GTest::gtest_main)
gtest_discover_tests(sdk_client_signer_test)

add_executable(sdk_proof_verifier_test proof_verifier_test.cpp)
target_link_libraries(sdk_proof_verifier_test PRIVATE sequencer::sdk GTest::gtest GTest::gtest_main)
gtest_discover_tests(sdk_proof_verifier_test)

add_executable(sdk_alarm_test alarm_test.cpp)
target_link_libraries(sdk_alarm_test PRIVATE sequencer::sdk GTest::gtest GTest::gtest_main)
gtest_discover_tests(sdk_alarm_test)

add_executable(sdk_reconciler_test reconciler_test.cpp)
target_link_libraries(sdk_reconciler_test PRIVATE sequencer::sdk GTest::gtest GTest::gtest_main)
gtest_discover_tests(sdk_reconciler_test)

add_executable(sdk_propose_client_test propose_client_test.cpp)
target_link_libraries(sdk_propose_client_test PRIVATE
  sequencer::sdk
  sequencer::gateway_input
  sequencer::node
  unofficial::brpc::brpc-static
  GTest::gtest
  GTest::gtest_main
  Threads::Threads
)
gtest_discover_tests(sdk_propose_client_test PROPERTIES TIMEOUT 60)
