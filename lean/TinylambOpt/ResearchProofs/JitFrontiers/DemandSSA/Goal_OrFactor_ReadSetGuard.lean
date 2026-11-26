import TinylambOpt.ResearchProofs.JitFrontiers.DemandSSA.Common_DemandSSA

/-!
SQL expression lowering must preserve which values and errors are demanded on each
active control-flow path.

Refuted by counterexample

- `OrFactor.ReadSetGuard` — A cached decision is reusable when all source and target
  read slots stay unchanged.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.DemandSSA.Goal_OrFactor_ReadSetGuard

open TinylambOpt.ResearchGoals.JitFrontiers.DemandSSA
open TinylambOpt.ResearchProofs.JitFrontiers.DemandSSA.Common

theorem refute : ¬ OrFactor.ReadSetGuard.Goal := by
  intro h
  have hb := h [.null, .yes, .fault 7] [.null, .yes, .fault 7] (by native_decide) (by intro i hi; rfl)
  have hbad : ¬ ((run [.null, .yes, .fault 7] OrFactor.rule.dst).1 = (run [.null, .yes, .fault 7] OrFactor.rule.src).1) := by native_decide
  exact hbad hb

end TinylambOpt.ResearchProofs.JitFrontiers.DemandSSA.Goal_OrFactor_ReadSetGuard
