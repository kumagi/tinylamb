#!/usr/bin/env bash
# Verify every registered Lean goal proof/counterexample and print coverage.
set -euo pipefail
cd "$(dirname "$0")/.."
export PATH="$HOME/.elan/bin:$PATH"

echo "[1/4] build StorageExecution proof aggregation"
lake build TinylambOpt.ResearchProofs.StorageExecution.AllGoalProofs >/dev/null

echo "[2/4] build JitFrontiers proof aggregation"
lake build TinylambOpt.ResearchProofs.JitFrontiers.AllGoalProofs >/dev/null

echo "[3/4] build auxiliary aggregations"
lake build \
  TinylambOpt.ResearchProofs.Compression.Text_All \
  TinylambOpt.ResearchProofs.Frontiers.WorkloadBases_All \
  TinylambOpt.ResearchProofs.Atlas.RangePlans_All \
  TinylambOpt.ResearchProofs.EngineBoundaries.AllGoalProofs \
  TinylambOpt.ResearchProofs.LayerContracts.AllGoalProofs >/dev/null

echo "[4/4] coverage audits"
lake env lean CheckStorageExecutionProofs.lean 2>&1 | grep -E 'StorageExecution audit|error' || true
lake env lean CheckJitFrontierProofs.lean 2>&1 | grep -E 'JitFrontiers audit|MISSING|CONFLICT|BADAX|error' || true
lake env lean CheckAllGoals.lean 2>&1 | grep -E '^ALL goals|MISSING-GROUP|BADAX|error' || true
