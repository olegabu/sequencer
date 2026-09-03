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

# Not defaulted to any particular path — every machine's vcpkg checkout
# lives wherever its owner put it (see README.md's Prerequisites: you
# clone it yourself and export VCPKG_ROOT, no fixed location is assumed
# anywhere else in this repo). If it's already exported in your shell,
# `?=` leaves it alone; check-vcpkg-root below fails loudly, not
# silently, for any target that actually needs it and doesn't have it.
VCPKG_ROOT ?=
export VCPKG_ROOT
export PATH := $(if $(VCPKG_ROOT),$(VCPKG_ROOT):)$(PATH)

# Which CMakePresets.json preset the default (unsuffixed) targets use.
PRESET ?= debug

# Passed to `ctest -R` when set, e.g. `make test TEST_FILTER=RelayGateway`.
TEST_FILTER ?=
CTEST_FLAGS := --output-on-failure
ifneq ($(strip $(TEST_FILTER)),)
CTEST_FLAGS += -R "$(TEST_FILTER)"
endif

.PHONY: help all check-vcpkg-root configure build test clean prune distclean \
        preflight debug test-debug \
        release test-release \
        tsan test-tsan \
        benchmark demo

# Make's default goal is otherwise whichever rule appears first in the
# file — `help` would win that race by accident. Bare `make` should
# build, like any other Makefile.
.DEFAULT_GOAL := all

help:
	@echo "Targets (default preset: $(PRESET); override with PRESET=release|tsan):"
	@echo "  make preflight         build+test the DEBUG preset, exactly as CI does"
	@echo "                         (run this before pushing; release+LTO hides link errors)"
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
	@echo "  make demo              examples/counter/demo_http_websocket.sh — curl + websocat, debug preset"
	@echo ""
	@echo "  make clean             remove build/\$$(PRESET) (all three if PRESET is unset: 'make clean')"
	@echo "  make prune             reclaim disk from already-built presets without an expensive"
	@echo "                         vcpkg dependency rebuild — see the prune target's own comment"
	@echo "                         (never touches vcpkg's downloads/ or packages/ — see why there)"
	@echo "  make distclean         clean, plus a full vcpkg reset (buildtrees/packages/downloads,"
	@echo "                         except downloads/tools/ — see distclean's own comment for why)"

all: build

# A clearer failure than what's left to discover otherwise: with
# VCPKG_ROOT empty, `cmake --preset` fails on a toolchain file path
# missing its prefix ("Could not find toolchain file:
# /scripts/buildsystems/vcpkg.cmake" — technically correct, not
# exactly obvious), and prune/distclean would construct an absolute,
# root-level path like /buildtrees from $(VCPKG_ROOT)/buildtrees and
# hand it straight to `rm -rf`. Never actually a real risk — no such
# directory exists — but exactly the shape of mistake not worth
# leaving unguarded, especially in a Makefile target whose entire job
# is deleting things.
check-vcpkg-root:
	@if [ -z "$(VCPKG_ROOT)" ]; then \
		echo 'error: VCPKG_ROOT is not set. Export it first -- e.g. "export VCPKG_ROOT=/path/to/your/vcpkg/checkout" -- see README.md'"'"'s Prerequisites' >&2; \
		exit 1; \
	fi

configure: check-vcpkg-root
	cmake --preset $(PRESET)

build: configure
	cmake --build --preset $(PRESET)

test: build
	ctest --preset $(PRESET) $(CTEST_FLAGS)

# What CI runs, run locally, before you push.
#
# CI builds the DEBUG preset. Fleet work builds the RELEASE preset,
# because that is what gets measured — and release links with LTO,
# which silently FOLDS duplicate symbols that debug rejects. A gflag
# defined in both a static library and a binary therefore linked
# cleanly here and failed there, twice, an hour after each push. Local
# green was never evidence about CI unless it was green in CI's own
# configuration.
#
# Deliberately not a pre-push git hook: this takes tens of minutes, and
# a hook that slow gets bypassed with --no-verify until it means
# nothing. It is a target you run when you are about to push, and the
# one command whose output settles the question.
.PHONY: preflight
preflight:
	@echo "== preflight: exactly what .github/workflows/ci.yml runs =="
	cmake --preset debug
	cmake --build --preset debug
	ctest --preset debug --output-on-failure
	@echo "== preflight passed: build and tests are green in CI's configuration =="

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
	./examples/counter/demo_http_websocket.sh

demo-grpc: debug
	./examples/counter/demo_grpc.sh

# `make clean` (no PRESET given) removes every preset's build
# directory; `make clean PRESET=release` removes just that one.
clean:
ifeq ($(origin PRESET), command line)
	rm -rf build/$(PRESET)
else
	rm -rf build/debug build/release build/tsan
endif

# Reclaims disk from presets you've already built, without paying
# `clean`'s cost of a from-scratch vcpkg dependency rebuild
# (braft/brpc/openssl/boost — the thing that can take the better part
# of an hour, per README.md's Build subsection): for each preset
# that's been configured, `ninja -t clean` removes just this project's
# own compiled object files and test binaries, leaving that preset's
# vcpkg_installed/ (the expensive part) untouched — the next build
# only has to recompile our own source, not the dependency stack. Also
# clears vcpkg's buildtrees/ (intermediate per-port build litter, safe
# to remove once a port is installed — reused as-is by every preset).
#
# Deliberately NOT touched here: vcpkg's downloads/ and packages/.
# They look like more of the same kind of reclaimable cache, but
# aren't: deleting them once broke a working build on this exact repo
# — vcpkg's own pinned CMake tool lives under downloads/tools/, needed
# because some older vcpkg ports (e.g. libevent) fail outright under a
# too-new system CMake (CMake >= 4.0 removed support for the very old
# cmake_minimum_required versions those ports still declare); losing
# it silently fell back to system CMake and broke the *next*
# `cmake --preset` reconfigure, not the build that deleted it, which
# is what made it easy to miss. packages/ is where vcpkg stages a
# port's build output before copying it into vcpkg_installed/, and
# vcpkg's install bookkeeping expects it to still be there even after
# a package is fully installed. Both are cheap enough (packages/ and
# downloads/ together are usually a fraction of buildtrees/'s size)
# that the disk saved isn't worth relitigating this — see distclean
# below if you actually want a from-scratch vcpkg reset.
prune: check-vcpkg-root
	@for preset in debug release tsan; do \
		if [ -f build/$$preset/build.ninja ]; then \
			echo "==> ninja -C build/$$preset -t clean (vcpkg_installed kept)"; \
			ninja -C build/$$preset -t clean; \
		fi; \
	done
	@echo "==> clearing vcpkg's buildtrees/ (downloads/ and packages/ deliberately left alone — see comment above)"
	rm -rf $(VCPKG_ROOT)/buildtrees

# Forces every dependency to rebuild from source on the next configure
# — clears buildtrees/, packages/, and downloads/'s cached port source
# tarballs. downloads/tools/ is deliberately spared even here: it's
# vcpkg's own pinned build tools (cmake, ninja, patchelf, ...), not
# port sources — see prune's comment above for what actually goes
# wrong if that's lost too (a working `cmake --preset` reconfigure
# breaking on the *next* run, not this one, which makes it a nasty one
# to track down).
distclean: clean check-vcpkg-root
	rm -rf $(VCPKG_ROOT)/buildtrees $(VCPKG_ROOT)/packages
	find $(VCPKG_ROOT)/downloads -mindepth 1 -maxdepth 1 ! -name tools -exec rm -rf {} +
