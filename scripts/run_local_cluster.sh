#!/usr/bin/env bash
# Roda 4 nós localmente sem Docker (útil enquanto o Docker não está instalado).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

PEERS="localhost:8000,localhost:8001,localhost:8002,localhost:8003"
PID_DIR="$ROOT/.local-cluster"
LOG_DIR="$PID_DIR/logs"

mkdir -p "$LOG_DIR"

stop_cluster() {
  echo "Parando cluster local..."
  for pidfile in "$PID_DIR"/node*.pid; do
    [[ -f "$pidfile" ]] || continue
    pid=$(cat "$pidfile")
    kill "$pid" 2>/dev/null || true
    rm -f "$pidfile"
  done
  echo "Cluster parado."
}

if [[ "${1:-}" == "stop" ]]; then
  stop_cluster
  exit 0
fi

if [[ "${1:-}" == "logs" ]]; then
  tail -f "$LOG_DIR"/*.log
  exit 0
fi

stop_cluster

echo "Iniciando cluster local (4 nós)..."
for id in 0 1 2 3; do
  NODE_ID="$id" NODE_COUNT=4 BASE_PORT=8000 PEERS="$PEERS" \
    RUN_MUTEX_DEMO="${RUN_MUTEX_DEMO:-true}" \
    DEMO_MODE="${DEMO_MODE:-mutex}" \
    COMMAND_DIR="${COMMAND_DIR:-shared/commands}" \
    uv run python src/main.py >"$LOG_DIR/node${id}.log" 2>&1 &
  echo $! >"$PID_DIR/node${id}.pid"
  echo "  node${id} -> pid $! (porta $((8000 + id)))"
done

echo ""
echo "Logs:  ./scripts/run_local_cluster.sh logs"
echo "Parar: ./scripts/run_local_cluster.sh stop"
