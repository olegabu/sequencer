# gateway/fix/ — the FIX session gateway (specification.md §8.10–§8.12).
# See README.md for why this directory owns a session layer instead of
# using a FIX engine; that question is asked often enough to deserve an
# answer beside the code.

# hffix is header-only and comes from the pinned vcpkg baseline
# (7e21420f, hffix 1.4.1) -- verified present rather than assumed, per
# specification.md §8.12's "via its vcpkg port if present in the pinned
# baseline, otherwise vendored".
find_path(HFFIX_INCLUDE_DIR NAMES hffix.hpp REQUIRED)

add_library(sequencer_fix_session STATIC
  session/src/fix_session.cpp
)
add_library(sequencer::fix_session ALIAS sequencer_fix_session)

target_include_directories(sequencer_fix_session
  PUBLIC
    "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/session/include>"
    ${HFFIX_INCLUDE_DIR}
)
target_compile_features(sequencer_fix_session PUBLIC cxx_std_20)

if(SEQUENCER_BUILD_TESTS)
  add_subdirectory(tests)
endif()
