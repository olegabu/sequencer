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

if(SEQUENCER_BUILD_TESTS)
  add_subdirectory(tests)
endif()
