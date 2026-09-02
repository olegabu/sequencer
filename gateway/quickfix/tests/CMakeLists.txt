add_executable(journal_message_store_test journal_message_store_test.cpp)
target_link_libraries(journal_message_store_test PRIVATE
  sequencer::quickfix_store
  GTest::gtest
  GTest::gtest_main
)
gtest_discover_tests(journal_message_store_test)
