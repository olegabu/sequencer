#!/usr/bin/env bash
# examples/counter/demo.sh — a live, external-tools walkthrough of the
# whole pipeline (specification.md §10): a real single-node raft group,
# a real input gateway, a real output gateway, driven entirely by tools
# outside this repository — curl for submission, websocat for
# WebSocket reception — so this demo genuinely proves the system speaks
# plain HTTP+JSON and plain WebSocket to any client, not just this
# repository's own C++ test code (see end_to_end_test.cpp for the
# equivalent driven from an in-process gtest instead).
#
# curl cannot itself receive over WebSocket: that support is
# experimental (curl >= 7.86, opt-in at compile time) and absent from
# most distro-packaged builds — this machine's own curl (7.68) predates
# it entirely. websocat (https://github.com/vi/websocat) is the "curl
# for WebSockets" equivalent used here instead.
#
# Usage: examples/counter/demo.sh [build_dir]   (default: build/debug)

set -euo pipefail

BUILD_DIR="${1:-build/debug}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

NODE_BIN="$REPO_ROOT/$BUILD_DIR/examples/counter/counter_node"
IG_BIN="$REPO_ROOT/$BUILD_DIR/examples/counter/counter_input_gateway"
OG_BIN="$REPO_ROOT/$BUILD_DIR/examples/counter/counter_output_gateway"

for bin in "$NODE_BIN" "$IG_BIN" "$OG_BIN"; do
  if [[ ! -x "$bin" ]]; then
    echo "error: $bin not found or not executable." >&2
    echo "Build first: cmake --preset debug && cmake --build --preset debug" >&2
    exit 1
  fi
done

if ! command -v websocat >/dev/null 2>&1; then
  echo "error: websocat not found on PATH." >&2
  echo "curl's own WebSocket support is experimental, opt-in at compile time, and absent from" >&2
  echo "most distro-packaged builds — install websocat instead:" >&2
  echo "  https://github.com/vi/websocat/releases" >&2
  exit 1
fi

NODE_PORT=9100
IG_PORT=9101
OG_PORT=9102

DATA_DIR="$(mktemp -d)"
RESUME_FILE="$DATA_DIR/resume"

NODE_PID=""
IG_PID=""
OG_PID=""
WS_PID=""

cleanup() {
  for pid in "$WS_PID" "$OG_PID" "$IG_PID" "$NODE_PID"; do
    [[ -n "$pid" ]] && kill "$pid" >/dev/null 2>&1 || true
  done
  wait >/dev/null 2>&1 || true
  rm -rf "$DATA_DIR"
}
trap cleanup EXIT

echo "== starting a single-node raft group (specification.md §10) =="
"$NODE_BIN" --peer="127.0.0.1:$NODE_PORT:0" --peers="127.0.0.1:$NODE_PORT:0" --data_dir="$DATA_DIR" \
  --logtostderr --logbufsecs=0 >"$DATA_DIR/node.log" 2>&1 &
NODE_PID=$!

echo "waiting for it to self-elect leader..."
elected=0
for _ in $(seq 1 100); do
  if curl -fs "http://127.0.0.1:$NODE_PORT/raft_stat" 2>/dev/null | grep -q "state: LEADER"; then
    elected=1
    break
  fi
  sleep 0.1
done
if [[ "$elected" -ne 1 ]]; then
  echo "error: node never became leader — see $DATA_DIR/node.log" >&2
  exit 1
fi
echo "leader elected."

echo "== starting the input gateway (curl target: http://127.0.0.1:$IG_PORT) =="
"$IG_BIN" --node_peers="127.0.0.1:$NODE_PORT" --listen_port="$IG_PORT" --logtostderr --logbufsecs=0 \
  >"$DATA_DIR/input_gateway.log" 2>&1 &
IG_PID=$!

echo "== starting the output gateway (websocat target: ws://127.0.0.1:$OG_PORT/totals) =="
"$OG_BIN" --data_dir="$DATA_DIR" --resume_file="$RESUME_FILE" --websocket_port="$OG_PORT" \
  --logtostderr --logbufsecs=0 >"$DATA_DIR/output_gateway.log" 2>&1 &
OG_PID=$!

sleep 0.5

echo "== connecting a websocat client to the output gateway =="
# -n (--no-close): this script gives websocat no stdin, so don't treat
# that EOF as a request to close the WebSocket — stay connected and
# just print whatever the server broadcasts.
timeout 15 websocat -n "ws://127.0.0.1:$OG_PORT/totals" </dev/null >"$DATA_DIR/ws_output.log" 2>&1 &
WS_PID=$!
sleep 0.5  # let the WebSocket handshake complete before anything is submitted

echo
echo "== submitting deltas with curl =="
DELTAS=(5 -2 10)
for delta in "${DELTAS[@]}"; do
  echo "> curl -X POST -H 'Content-Type: application/json' -d '{\"delta\": $delta}' http://127.0.0.1:$IG_PORT/SubmitService/Submit"
  response="$(curl -s -X POST -H 'Content-Type: application/json' -d "{\"delta\": $delta}" \
    "http://127.0.0.1:$IG_PORT/SubmitService/Submit")"
  echo "< $response"
  sleep 0.3
done

sleep 1
kill "$WS_PID" >/dev/null 2>&1 || true
wait "$WS_PID" 2>/dev/null || true
WS_PID=""

echo
echo "== broadcasts received over the WebSocket (websocat -n ws://127.0.0.1:$OG_PORT/totals) =="
cat "$DATA_DIR/ws_output.log"

expected=${#DELTAS[@]}
actual=$(wc -l <"$DATA_DIR/ws_output.log")
echo
if [[ "$actual" -eq "$expected" ]]; then
  echo "OK: received $actual/$expected broadcasts over the WebSocket, matching the curl submissions above."
else
  echo "WARNING: expected $expected broadcasts, received $actual — see $DATA_DIR/*.log" >&2
  exit 1
fi
