#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

TARGET_ANSWERS="${1:-200}"

export SWARM_AGENTS="${SWARM_AGENTS:-12}"
export SWARM_POLL_MS="${SWARM_POLL_MS:-1000}"
export SWARM_IDLE_MS="${SWARM_IDLE_MS:-1800}"
export SWARM_INCLUDE_LOW_ANSWER="${SWARM_INCLUDE_LOW_ANSWER:-true}"
export SWARM_TARGET_MAX_ANSWERS="${SWARM_TARGET_MAX_ANSWERS:-4}"
export SWARM_SEED_WHEN_IDLE="${SWARM_SEED_WHEN_IDLE:-true}"
export SWARM_SEED_AFTER_IDLE_LOOPS="${SWARM_SEED_AFTER_IDLE_LOOPS:-1}"
export SWARM_USE_ANSWER_TOOL="${SWARM_USE_ANSWER_TOOL:-true}"
export SWARM_ROTATE_TRIAL_ON_LIMIT="${SWARM_ROTATE_TRIAL_ON_LIMIT:-true}"
export SWARM_LIMIT_COOLDOWN_MS="${SWARM_LIMIT_COOLDOWN_MS:-60000}"
export SWARM_MAX_ANSWERS_TOTAL="${SWARM_MAX_ANSWERS_TOTAL:-$TARGET_ANSWERS}"

mkdir -p .ai/logs
STAMP="$(date +%Y%m%d-%H%M%S)"
LOG_FILE=".ai/logs/agent-swarm-burst-${STAMP}.log"

echo "Starting capped swarm burst..."
echo "  target answers: ${SWARM_MAX_ANSWERS_TOTAL}"
echo "  agents: ${SWARM_AGENTS}"
echo "  log: ${LOG_FILE}"

pnpm exec tsx scripts/agent-swarm.ts 2>&1 | tee "$LOG_FILE"
