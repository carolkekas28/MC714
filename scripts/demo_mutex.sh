#!/usr/bin/env bash
# Demonstrates Ricart-Agrawala mutual exclusion.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

MODE="${1:-local}"
DURATION="${2:-45}"

if [[ "$MODE" == "local" ]]; then
  rm -f shared/critical.log
  mkdir -p shared
  ./scripts/run_local_cluster.sh stop 2>/dev/null || true
  ./scripts/run_local_cluster.sh
  echo "Running mutex demo for ${DURATION}s..."
  sleep "$DURATION"
  echo ""
  echo "=== Critical section log (should have no overlapping entries) ==="
  if [[ -f shared/critical.log ]]; then
    cat shared/critical.log
  else
    echo "(no entries yet)"
  fi
  echo ""
  echo "=== Recent mutex events ==="
  grep -h "REQUEST critical section\|ENTER critical\|EXIT critical\|Deferred" \
    .local-cluster/logs/*.log 2>/dev/null | tail -30 || true
  ./scripts/run_local_cluster.sh stop
elif [[ "$MODE" == "docker" ]]; then
  docker compose down 2>/dev/null || true
  rm -f shared/critical.log
  mkdir -p shared
  docker compose up --build -d
  echo "Running mutex demo for ${DURATION}s..."
  sleep "$DURATION"
  echo ""
  echo "=== Critical section log ==="
  docker compose exec node0 cat /app/shared/critical.log 2>/dev/null || \
    cat shared/critical.log 2>/dev/null || echo "(no entries yet)"
  echo ""
  echo "=== Recent mutex events ==="
  docker compose logs --no-color | \
    grep -E "REQUEST critical section|ENTER critical|EXIT critical|Deferred" | tail -30
  docker compose down
else
  echo "Usage: $0 [local|docker] [seconds]"
  exit 1
fi
