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
