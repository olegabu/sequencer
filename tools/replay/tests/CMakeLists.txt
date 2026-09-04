include(GoogleTest)

add_executable(replay_check_test replay_check_test.cpp)
target_link_libraries(replay_check_test PRIVATE sequencer::replay GTest::gtest GTest::gtest_main sequencer::common)
gtest_discover_tests(replay_check_test)
