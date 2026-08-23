# journal/ — header-only, depends on nothing (specification.md §9.1).
add_library(sequencer_journal INTERFACE)
add_library(sequencer::journal ALIAS sequencer_journal)

target_include_directories(sequencer_journal INTERFACE
  "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
)

target_compile_features(sequencer_journal INTERFACE cxx_std_20)

if(SEQUENCER_BUILD_TESTS)
  add_subdirectory(tests)
endif()

if(SEQUENCER_BUILD_BENCHMARKS)
  add_subdirectory(benchmarks)
endif()
