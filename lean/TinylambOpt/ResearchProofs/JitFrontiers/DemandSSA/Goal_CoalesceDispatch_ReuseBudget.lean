import TinylambOpt.ResearchProofs.JitFrontiers.DemandSSA.Common_DemandSSA

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.DemandSSA.Goal_CoalesceDispatch_ReuseBudget

open TinylambOpt.ResearchGoals.JitFrontiers.DemandSSA
open TinylambOpt.ResearchProofs.JitFrontiers.DemandSSA.Common

theorem refute : ¬ CoalesceDispatch.ReuseBudget.Goal := by
  intro h
  have hb := h [.yes, .yes] 0 (by native_decide)
  have hbad : ¬ ((run [.yes, .yes] CoalesceDispatch.rule.dst).2.count 0 ≤ (run [.yes, .yes] CoalesceDispatch.rule.src).2.count 0) := by native_decide
  exact hbad hb

end TinylambOpt.ResearchProofs.JitFrontiers.DemandSSA.Goal_CoalesceDispatch_ReuseBudget
