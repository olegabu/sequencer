find_package(Threads REQUIRED)
include(GoogleTest)

add_executable(output_gateway_test output_gateway_test.cpp)
target_link_libraries(output_gateway_test PRIVATE sequencer::gateway_output GTest::gtest GTest::gtest_main Threads::Threads)
gtest_discover_tests(output_gateway_test)
