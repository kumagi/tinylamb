import TinylambOpt.ResearchProofs.JitFrontiers.DemandSSA.Common_DemandSSA

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.DemandSSA.Goal_AndCommute_ReuseBudget

open TinylambOpt.ResearchGoals.JitFrontiers.DemandSSA
open TinylambOpt.ResearchProofs.JitFrontiers.DemandSSA.Common

theorem refute : ¬ AndCommute.ReuseBudget.Goal := by
  intro h
  have hb := h [.no, .yes] 1 (by native_decide)
  have hbad : ¬ ((run [.no, .yes] AndCommute.rule.dst).2.count 1 ≤ (run [.no, .yes] AndCommute.rule.src).2.count 1) := by native_decide
  exact hbad hb

end TinylambOpt.ResearchProofs.JitFrontiers.DemandSSA.Goal_AndCommute_ReuseBudget
