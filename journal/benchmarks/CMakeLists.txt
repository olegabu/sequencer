add_executable(journal_benchmark journal_benchmark.cpp)
target_link_libraries(journal_benchmark PRIVATE sequencer::journal benchmark::benchmark sequencer::common)
