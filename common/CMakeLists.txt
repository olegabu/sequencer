# Small header-only utilities shared across components.
#
# INTERFACE targets, deliberately: everything here is inline, so there
# are no object files to travel into binaries that did not ask for them.
# That is the same narrowest-dependency rule as sequencer::output_codec_api
# and sequencer::fix_session_api next door, and for the same reason --
# this repository has twice had a build broken by a static library
# carrying program-level symbols into a link that only wanted a
# declaration.
#
# The bar for adding something here: used by two or more components,
# and no component-specific dependencies. Anything that needs a library
# belongs to the component that owns it, not here.
add_library(sequencer_common INTERFACE)
add_library(sequencer::common ALIAS sequencer_common)
target_include_directories(sequencer_common
  INTERFACE "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>")
target_compile_features(sequencer_common INTERFACE cxx_std_20)
