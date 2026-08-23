# Makefile — a thin convenience wrapper around this project's CMake
# presets (CMakePresets.json: debug, release, tsan). See README.md's
# Development section for the full manual command sequence this
# automates, and docs/testing.md for what the test suite actually
# covers.
#
# This is deliberately the only Makefile in the repository. CMake's
# find_package() calls for braft/brpc/protobuf/etc. (and the compiler,
# toolchain file, and vcpkg manifest) live only in the top-level
# CMakeLists.txt (specification.md §9's dependency graph) — no
# subdirectory here is independently configurable, so a per-component
# Makefile would either duplicate this one or not actually work
# standalone. Filter to one component's tests instead with
# TEST_FILTER (see `make help`).

VCPKG_ROOT ?= $(HOME)/workspace/vcpkg
export VCPKG_ROOT
export PATH := $(VCPKG_ROOT):$(PATH)

# Which CMakePresets.json preset the default (unsuffixed) targets use.
PRESET ?= debug

# Passed to `ctest -R` when set, e.g. `make test TEST_FILTER=RelayGateway`.
TEST_FILTER ?=
CTEST_FLAGS := --output-on-failure
ifneq ($(strip $(TEST_FILTER)),)
CTEST_FLAGS += -R "$(TEST_FILTER)"
endif

.PHONY: help all configure build test clean distclean \
        debug test-debug \
        release test-release \
        tsan test-tsan \
        benchmark demo

# Make's default goal is otherwise whichever rule appears first in the
# file — `help` would win that race by accident. Bare `make` should
# build, like any other Makefile.
.DEFAULT_GOAL := all

help:
	@echo "Targets (default preset: $(PRESET); override with PRESET=release|tsan):"
	@echo "  make configure         cmake --preset \$$(PRESET)"
	@echo "  make build             configure, then cmake --build --preset \$$(PRESET)"
	@echo "  make test              build, then ctest --preset \$$(PRESET) --output-on-failure"
	@echo "  make all               alias for 'build' (the default target)"
	@echo ""
	@echo "  make debug             build, debug preset (Debug, the everyday workflow)"
	@echo "  make test-debug        test, debug preset"
	@echo "  make release           build, release preset (LTO — specification.md §9.1)"
	@echo "  make test-release      test, release preset"
	@echo "  make tsan              build, tsan preset (ThreadSanitizer)"
	@echo "  make test-tsan         test, tsan preset — the journal's §6.3 cross-thread protocol"
	@echo ""
	@echo "  make test TEST_FILTER=RelayGateway   filter to matching test names (ctest -R)"
	@echo ""
	@echo "  make benchmark         run journal/'s micro-benchmarks (debug preset)"
	@echo "  make demo              examples/counter/demo.sh — curl + websocat, debug preset"
	@echo ""
	@echo "  make clean             remove build/\$$(PRESET) (all three if PRESET is unset: 'make clean')"
	@echo "  make distclean         clean, plus vcpkg's own build cache (buildtrees/downloads/packages)"

all: build

configure:
	cmake --preset $(PRESET)

build: configure
	cmake --build --preset $(PRESET)

test: build
	ctest --preset $(PRESET) $(CTEST_FLAGS)

# Named shortcuts — the three presets this project defines.
debug:
	$(MAKE) build PRESET=debug
test-debug:
	$(MAKE) test PRESET=debug

release:
	$(MAKE) build PRESET=release
test-release:
	$(MAKE) test PRESET=release

tsan:
	$(MAKE) build PRESET=tsan
test-tsan:
	$(MAKE) test PRESET=tsan

benchmark: debug
	./build/debug/journal/benchmarks/journal_benchmark

demo: debug
	./examples/counter/demo.sh

# `make clean` (no PRESET given) removes every preset's build
# directory; `make clean PRESET=release` removes just that one.
clean:
ifeq ($(origin PRESET), command line)
	rm -rf build/$(PRESET)
else
	rm -rf build/debug build/release build/tsan
endif

distclean: clean
	rm -rf $(VCPKG_ROOT)/buildtrees $(VCPKG_ROOT)/downloads $(VCPKG_ROOT)/packages
