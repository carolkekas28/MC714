#!/usr/bin/env bash
# Demonstrates Bully leader election by stopping the current leader container/process.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

MODE="${1:-docker}"
LEADER="${2:-node3}"

if [[ "$MODE" == "docker" ]]; then
  docker compose down 2>/dev/null || true
  RUN_MUTEX_DEMO=false docker compose up --build -d
  echo "Waiting for cluster and initial coordinator..."
  sleep 25

  echo ""
  echo "=== Leader before failure ==="
  docker compose logs --no-color | grep -E "coordinator|Accepted node" | tail -8

  echo ""
  echo "Stopping ${LEADER} (expected leader)..."
  docker compose stop "$LEADER"

  echo "Waiting for re-election..."
  sleep 8

  echo ""
  echo "=== Election events ==="
  docker compose logs --no-color | \
    grep -E "Starting election|Accepted node|I am the new coordinator|Leader node.*timed out" | tail -20

  docker compose down
elif [[ "$MODE" == "local" ]]; then
  ./scripts/run_local_cluster.sh stop 2>/dev/null || true
  RUN_MUTEX_DEMO=false ./scripts/run_local_cluster.sh
  echo "Waiting for cluster and initial coordinator..."
  sleep 20

  echo ""
  echo "=== Leader before failure ==="
  grep -h "coordinator\|Accepted node" .local-cluster/logs/*.log | tail -8

  LEADER_ID="${LEADER#node}"
  echo ""
  echo "Stopping node${LEADER_ID}..."
  kill "$(cat ".local-cluster/node${LEADER_ID}.pid")" 2>/dev/null || true

  echo "Waiting for re-election..."
  sleep 8

  echo ""
  echo "=== Election events ==="
  grep -h "Starting election\|Accepted node\|I am the new coordinator\|Leader node.*timed out" \
    .local-cluster/logs/*.log | tail -20

  ./scripts/run_local_cluster.sh stop
else
  echo "Usage: $0 [docker|local] [node3|node2|...]"
  exit 1
fi
