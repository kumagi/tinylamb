import TinylambOpt.ResearchProofs.JitFrontiers.DemandSSA.Common_DemandSSA

/-!
SQL expression lowering must preserve which values and errors are demanded on each
active control-flow path.

Refuted by counterexample

- `AndCommute.NoNewRead` — Guarded lowering must not touch a slot skipped by scalar
  short circuit.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.DemandSSA.Goal_AndCommute_NoNewRead

open TinylambOpt.ResearchGoals.JitFrontiers.DemandSSA
open TinylambOpt.ResearchProofs.JitFrontiers.DemandSSA.Common

theorem refute : ¬ AndCommute.NoNewRead.Goal := by
  intro h
  have hb := h [.no, .yes] (by native_decide) 1 (by native_decide)
  have hbad : ¬ (1 ∈ (run [.no, .yes] AndCommute.rule.src).2) := by native_decide
  exact hbad hb

end TinylambOpt.ResearchProofs.JitFrontiers.DemandSSA.Goal_AndCommute_NoNewRead
