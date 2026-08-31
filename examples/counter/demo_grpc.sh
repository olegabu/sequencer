#!/usr/bin/env bash
# examples/counter/demo_grpc.sh — the real-gRPC counterpart to
# demo_rest_websocket.sh: the same single-node raft group, but
# submission and dissemination both go over real gRPC
# (grpc-go/grpc-java/grpcurl-compatible, unlike brpc's own Streaming
# RPC), driven entirely by grpcurl — no brpc, no custom client code, no
# `.proto` file needed on the grpcurl command line either, since both
# gRPC services here enable server reflection.
#
# READ THIS BEFORE COMPARING THE TWO DEMOS. They differ on TWO axes at
# once, and only one of them is in this script's name:
#
#                    demo_rest_websocket.sh      demo_grpc.sh
#   input gateway    generic chassis, opaque     typed, own schema
#   submit wire      HTTP + JSON (curl)          gRPC (grpcurl)
#   output transport WebSocket                   real gRPC
#   receive wire     WebSocket (websocat)        gRPC (grpcurl)
#
# So a difference you notice between them may come from the INPUT
# GATEWAY ARCHITECTURE rather than from gRPC. Both gateways propose
# identically underneath -- same NodeProposer, same async handoff, same
# batching -- so the difference is in the client-facing contract, not
# the path to the raft group.
#
# Two parallel patterns, on purpose (see README.md for the tradeoff):
#   - Output: sequencer::GrpcOutputTransport (gateway/output/) — the
#     *generic*, reusable gRPC OutputTransport, "just like" the
#     WebSocket one demo_rest_websocket.sh uses. It wraps whatever
#     bytes CounterOutputCodec already produces (the same JSON that
#     demo shows you over WebSocket) in a generic bytes envelope — so
#     grpcurl prints it base64-encoded, which this script decodes for
#     you. One message is an OutputRecordBatch carrying a REPEATED
#     payloads field, because every output transport batches whatever
#     the subscriber's reader found available; how many records land in
#     one message is not something a subscriber controls.
#   - Input: a small, counter-specific gRPC service
#     (CounterSubmitService, proto/counter_input_grpc.proto) with real,
#     distinct fields (delta in, sequence_number/total out) — the
#     fully-typed alternative pattern, demonstrating how an application
#     goes further than the generic envelope when it wants grpcurl to
#     print real business fields directly. Note this is the CLIENT
#     contract only: one delta per call. The batching above happens
#     between gateway and node, invisibly to this client.
#
# Usage: examples/counter/demo_grpc.sh [build_dir]   (default: build/debug)

set -euo pipefail

BUILD_DIR="${1:-build/debug}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

NODE_BIN="$REPO_ROOT/$BUILD_DIR/examples/counter/counter_node"
IG_BIN="$REPO_ROOT/$BUILD_DIR/examples/counter/counter_grpc_input_gateway"
OG_BIN="$REPO_ROOT/$BUILD_DIR/examples/counter/counter_output_gateway"

for bin in "$NODE_BIN" "$IG_BIN" "$OG_BIN"; do
  if [[ ! -x "$bin" ]]; then
    echo "error: $bin not found or not executable." >&2
    echo "Build first: cmake --preset debug && cmake --build --preset debug" >&2
    exit 1
  fi
done

if ! command -v grpcurl >/dev/null 2>&1; then
  echo "error: grpcurl not found on PATH." >&2
  echo "install it from: https://github.com/fullstorydev/grpcurl/releases" >&2
  exit 1
fi

NODE_PORT=9200
IG_PORT=9201
OG_PORT=9202

DATA_DIR="$(mktemp -d)"
RESUME_FILE="$DATA_DIR/resume"

NODE_PID=""
IG_PID=""
OG_PID=""
GRPCURL_PID=""

cleanup() {
  for pid in "$GRPCURL_PID" "$OG_PID" "$IG_PID" "$NODE_PID"; do
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

echo "== starting the gRPC input gateway (grpcurl target: 127.0.0.1:$IG_PORT sequencer.examples.counter.grpc_proto.CounterSubmitService/SubmitDelta) =="
"$IG_BIN" --node_peers="127.0.0.1:$NODE_PORT" --listen_port="$IG_PORT" --logtostderr --logbufsecs=0 \
  >"$DATA_DIR/input_gateway.log" 2>&1 &
IG_PID=$!

echo "== starting the gRPC output gateway (grpcurl target: 127.0.0.1:$OG_PORT sequencer.gateway.output.grpc_proto.GenericOutputService/Subscribe) =="
"$OG_BIN" --data_dir="$DATA_DIR" --resume_file="$RESUME_FILE" --grpc_port="$OG_PORT" \
  --logtostderr --logbufsecs=0 >"$DATA_DIR/output_gateway.log" 2>&1 &
OG_PID=$!

sleep 0.5

echo "== connecting a grpcurl client to the output stream =="
# grpcurl's server-streaming calls just keep printing JSON objects, one
# per received message, until the stream ends or grpcurl is killed —
# no special flag needed, unlike websocat's -n/--no-close.
grpcurl -plaintext -d '{"topic":"totals"}' "127.0.0.1:$OG_PORT" \
  sequencer.gateway.output.grpc_proto.GenericOutputService/Subscribe \
  >"$DATA_DIR/grpc_output.log" 2>&1 &
GRPCURL_PID=$!
sleep 0.5  # let the gRPC stream's subscription register before anything is submitted

echo
echo "== submitting deltas with grpcurl =="
DELTAS=(5 -2 10)
for delta in "${DELTAS[@]}"; do
  echo "> grpcurl -plaintext -d '{\"delta\": $delta}' 127.0.0.1:$IG_PORT sequencer.examples.counter.grpc_proto.CounterSubmitService/SubmitDelta"
  response="$(grpcurl -plaintext -d "{\"delta\": $delta}" "127.0.0.1:$IG_PORT" \
    sequencer.examples.counter.grpc_proto.CounterSubmitService/SubmitDelta)"
  echo "< $(echo "$response" | tr '\n' ' ')"
  sleep 0.3
done

sleep 1
kill "$GRPCURL_PID" >/dev/null 2>&1 || true
wait "$GRPCURL_PID" 2>/dev/null || true
GRPCURL_PID=""

echo
echo "== raw grpcurl output (each OutputRecordBatch.payloads entry is base64 — protobuf's own"
echo "   JSON mapping for a \"bytes\" field, not something specific to this transport) =="
cat "$DATA_DIR/grpc_output.log"

echo
echo "== decoded payloads (the same JSON demo_rest_websocket.sh's WebSocket path shows you) =="
python3 - "$DATA_DIR/grpc_output.log" <<'PYEOF'
import base64
import json
import sys

path = sys.argv[1]
count = 0
with open(path) as f:
    text = f.read()
for chunk in text.split("}\n{"):
    chunk = chunk.strip()
    if not chunk:
        continue
    chunk = chunk if chunk.startswith("{") else "{" + chunk
    chunk = chunk if chunk.endswith("}") else chunk + "}"
    obj = json.loads(chunk)
    # One OutputRecordBatch carries a repeated "payloads" field, so a
    # single message may hold several records -- how many depends on
    # what the reader found available in the ring, not on anything the
    # subscriber controls (see gateway/output/README.md's batching
    # note). Older single-record "payload" is accepted too.
    payloads = obj.get("payloads")
    if payloads is None:
        payloads = [obj["payload"]] if "payload" in obj else []
    for encoded in payloads:
        print(base64.b64decode(encoded).decode("utf-8"))
        count += 1
print(f"\n{count} record(s) decoded", file=sys.stderr)
PYEOF

expected=${#DELTAS[@]}
# Counts RECORDS, not messages: grep -c would count batches, and one
# batch can carry several records.
actual=$(python3 -c '
import base64, json, sys
text = open(sys.argv[1]).read()
n = 0
for chunk in text.split("}\n{"):
    chunk = chunk.strip()
    if not chunk:
        continue
    chunk = chunk if chunk.startswith("{") else "{" + chunk
    chunk = chunk if chunk.endswith("}") else chunk + "}"
    obj = json.loads(chunk)
    n += len(obj.get("payloads", [obj["payload"]] if "payload" in obj else []))
print(n)
' "$DATA_DIR/grpc_output.log" 2>/dev/null || echo 0)
echo
if [[ "$actual" -eq "$expected" ]]; then
  echo "OK: received $actual/$expected records over gRPC, matching the grpcurl submissions above."
else
  echo "WARNING: expected $expected records, received $actual — see $DATA_DIR/*.log" >&2
  exit 1
fi
