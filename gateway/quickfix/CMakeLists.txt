# gateway/quickfix/ — the alternative FIX gateway, on QuickFIX's session
# layer instead of this repository's own (specification.md §8.13).
#
# Not a replacement for gateway/fix/: both are built, and README.md
# there states when each is the right choice. The name is the
# distinction -- "quickfix" is the engine-backed one, "fix" is the
# hffix-and-owned-session-core one.
#
# NOTE: this makes quickfix a PRODUCTION dependency, which it was not
# before (§8.12 kept it test-only, and gateway/fix/tests is still the
# only place gateway/fix/ touches it). That is a deliberate consequence
# of §8.13 and is why this lives in its own directory with its own
# target rather than being folded into gateway/fix/.
find_package(quickfix CONFIG QUIET)
if(NOT quickfix_FOUND)
  find_library(QUICKFIX_LIBRARY NAMES quickfix REQUIRED)
  find_path(QUICKFIX_INCLUDE_DIR NAMES quickfix/Session.h REQUIRED)
endif()

add_library(sequencer_quickfix_store STATIC
  store/src/journal_message_store.cpp
)
add_library(sequencer::quickfix_store ALIAS sequencer_quickfix_store)
target_include_directories(sequencer_quickfix_store PUBLIC
  "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/store/include>"
)
if(quickfix_FOUND)
  target_link_libraries(sequencer_quickfix_store PUBLIC quickfix)
else()
  target_include_directories(sequencer_quickfix_store PUBLIC ${QUICKFIX_INCLUDE_DIR})
  target_link_libraries(sequencer_quickfix_store PUBLIC ${QUICKFIX_LIBRARY})
endif()
target_link_libraries(sequencer_quickfix_store PUBLIC sequencer::output_codec_api)
target_compile_features(sequencer_quickfix_store PUBLIC cxx_std_20)

add_library(sequencer_quickfix_input STATIC
  input/src/quickfix_input_transport.cpp
)
add_library(sequencer::quickfix_input ALIAS sequencer_quickfix_input)
target_include_directories(sequencer_quickfix_input PUBLIC
  "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/input/include>"
)
target_link_libraries(sequencer_quickfix_input PUBLIC
  sequencer::quickfix_store
  sequencer::gateway_input
  sequencer::fix_session_api
)
target_compile_features(sequencer_quickfix_input PUBLIC cxx_std_20)

add_library(sequencer_quickfix_output STATIC
  output/src/quickfix_output_transport.cpp
)
add_library(sequencer::quickfix_output ALIAS sequencer_quickfix_output)
target_include_directories(sequencer_quickfix_output PUBLIC
  "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/output/include>"
)
# output_codec_api, NOT gateway_output: the latter also contains
# run_output_gateway.cpp, whose DEFINE_string(data_dir, ...) and
# DEFINE_string(resume_file, ...) are program-level gflags at namespace
# scope. counter_quickfix_gateway defines both flags for itself, so
# pulling that object in gave two definitions of one symbol and CI's
# linker refused it -- the same failure this directory's neighbour
# documents at gateway/output/CMakeLists.txt, hit again by new code.
#
# It went unnoticed locally because the release preset builds with LTO,
# which folded the duplicate; only the debug preset CI uses reproduces
# it. `cmake --build --preset debug` is the check that matters here.
target_link_libraries(sequencer_quickfix_output PUBLIC
  sequencer::quickfix_input
  sequencer::output_codec_api
)
target_compile_features(sequencer_quickfix_output PUBLIC cxx_std_20)

add_library(sequencer_quickfix_gateway STATIC
  src/quickfix_session_gateway.cpp
)
add_library(sequencer::quickfix_gateway ALIAS sequencer_quickfix_gateway)
target_include_directories(sequencer_quickfix_gateway PUBLIC
  "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
)
target_link_libraries(sequencer_quickfix_gateway PUBLIC
  sequencer::quickfix_input
  sequencer::quickfix_output
  sequencer::common
)
target_compile_features(sequencer_quickfix_gateway PUBLIC cxx_std_20)

if(SEQUENCER_BUILD_TESTS)
  add_subdirectory(tests)
endif()
