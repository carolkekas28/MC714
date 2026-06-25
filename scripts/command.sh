#!/usr/bin/env bash
# Envia comando para um nó via arquivo compartilhado (Docker ou cluster local).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

if [[ $# -lt 2 ]]; then
  echo "Usage: $0 <node0|node1|node2|node3> <command>"
  echo "Examples:"
  echo "  $0 node0 status"
  echo "  $0 node1 request-cs"
  echo "  $0 node2 event 3"
  exit 1
fi

SERVICE="$1"
shift
COMMAND="$*"
COMMAND_DIR="${COMMAND_DIR:-shared/commands}"

mkdir -p "$COMMAND_DIR"
printf '%s\n' "$COMMAND" > "$COMMAND_DIR/${SERVICE}.cmd"
echo "Sent to ${SERVICE}: ${COMMAND}"
echo "Check logs: docker compose logs -f ${SERVICE}"
