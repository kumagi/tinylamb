import TinylambOpt.ResearchProofs.JitFrontiers.DemandSSA.Common_DemandSSA

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.DemandSSA.Goal_AndCommute_DropGuardInFilter

open TinylambOpt.ResearchGoals.JitFrontiers.DemandSSA
open TinylambOpt.ResearchProofs.JitFrontiers.DemandSSA.Common

theorem refute : ¬ AndCommute.DropGuardInFilter.Goal := by
  intro h
  have hb := h [.no, .fault 7]
  have hbad : ¬ (accepted (run [.no, .fault 7] AndCommute.rule.dst).1 = accepted (run [.no, .fault 7] AndCommute.rule.src).1) := by native_decide
  exact hbad hb

end TinylambOpt.ResearchProofs.JitFrontiers.DemandSSA.Goal_AndCommute_DropGuardInFilter
