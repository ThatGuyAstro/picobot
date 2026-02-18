#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

BIN="./picobot"
go build -o "$BIN" ./cmd/picobot

cleanup() {
  for pid in "${PIDS[@]:-}"; do
    kill "$pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true
}
trap cleanup EXIT

PIDS=()

"$BIN" swarm orchestrator --listen :8080 --task-timeout 30s --retries 3 > /tmp/picobot-orchestrator.log 2>&1 &
PIDS+=("$!")

"$BIN" swarm node --node-id node-analysis --listen :8091 --public-url http://127.0.0.1:8091 --orchestrator-url http://127.0.0.1:8080 --specializations analysis > /tmp/picobot-node-analysis.log 2>&1 &
PIDS+=("$!")

"$BIN" swarm node --node-id node-generation --listen :8092 --public-url http://127.0.0.1:8092 --orchestrator-url http://127.0.0.1:8080 --specializations generation > /tmp/picobot-node-generation.log 2>&1 &
PIDS+=("$!")

"$BIN" swarm node --node-id node-validation --listen :8093 --public-url http://127.0.0.1:8093 --orchestrator-url http://127.0.0.1:8080 --specializations validation,synthesis > /tmp/picobot-node-validation.log 2>&1 &
PIDS+=("$!")

sleep 3

"$BIN" swarm submit \
  --orchestrator-url http://127.0.0.1:8080 \
  --file examples/swarm/high-order-task.json
