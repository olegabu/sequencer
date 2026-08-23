find_package(Threads REQUIRED)
include(GoogleTest)

add_executable(input_gateway_test input_gateway_test.cpp)
target_link_libraries(input_gateway_test PRIVATE sequencer::gateway_input sequencer::node GTest::gtest GTest::gtest_main Threads::Threads)
gtest_discover_tests(input_gateway_test)
