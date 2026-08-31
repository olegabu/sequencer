find_package(Threads REQUIRED)
include(GoogleTest)

add_executable(fix_session_test fix_session_test.cpp)
target_link_libraries(fix_session_test PRIVATE sequencer::fix_session
                                                GTest::gtest GTest::gtest_main Threads::Threads)
gtest_discover_tests(fix_session_test)
