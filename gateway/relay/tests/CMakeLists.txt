find_package(Threads REQUIRED)
include(GoogleTest)

add_executable(relay_gateway_test relay_gateway_test.cpp)
target_link_libraries(relay_gateway_test PRIVATE
  sequencer::relay_proto
  sequencer::relay_client
  unofficial::brpc::brpc-static
  GTest::gtest
  GTest::gtest_main
  Threads::Threads
)
gtest_discover_tests(relay_gateway_test PROPERTIES TIMEOUT 60)
