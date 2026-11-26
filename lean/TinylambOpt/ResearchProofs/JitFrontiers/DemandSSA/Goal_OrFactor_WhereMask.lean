import TinylambOpt.ResearchProofs.JitFrontiers.DemandSSA.Common_DemandSSA

/-!
SQL expression lowering must preserve which values and errors are demanded on each
active control-flow path.

Refuted by counterexample

- `OrFactor.WhereMask` — WHERE merges FALSE and UNKNOWN, but still observes faults.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.DemandSSA.Goal_OrFactor_WhereMask

open TinylambOpt.ResearchGoals.JitFrontiers.DemandSSA
open TinylambOpt.ResearchProofs.JitFrontiers.DemandSSA.Common

theorem refute : ¬ OrFactor.WhereMask.Goal := by
  intro h
  have hb := h [.null, .yes, .fault 7] (by native_decide)
  have hbad : ¬ (accepted (run [.null, .yes, .fault 7] OrFactor.rule.dst).1 = accepted (run [.null, .yes, .fault 7] OrFactor.rule.src).1) := by native_decide
  exact hbad hb

end TinylambOpt.ResearchProofs.JitFrontiers.DemandSSA.Goal_OrFactor_WhereMask
