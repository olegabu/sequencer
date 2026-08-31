find_package(Threads REQUIRED)
include(GoogleTest)

function(sequencer_journal_test name)
  add_executable(${name} ${name}.cpp)
  target_link_libraries(${name} PRIVATE sequencer::journal GTest::gtest GTest::gtest_main Threads::Threads)
  gtest_discover_tests(${name})
endfunction()

sequencer_journal_test(format_test)
sequencer_journal_test(writer_reader_test)
sequencer_journal_test(recovery_test)
sequencer_journal_test(concurrency_test)
sequencer_journal_test(output_collector_test)
