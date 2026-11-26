import TinylambOpt.ResearchProofs.JitFrontiers.DemandSSA.Common_DemandSSA

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.DemandSSA.Goal_AbsorbAnd_DropGuardInFilter

open TinylambOpt.ResearchGoals.JitFrontiers.DemandSSA
open TinylambOpt.ResearchProofs.JitFrontiers.DemandSSA.Common

theorem refute : ¬ AbsorbAnd.DropGuardInFilter.Goal := by
  intro h
  have hb := h [.null, .fault 7]
  have hbad : ¬ (accepted (run [.null, .fault 7] AbsorbAnd.rule.dst).1 = accepted (run [.null, .fault 7] AbsorbAnd.rule.src).1) := by native_decide
  exact hbad hb

end TinylambOpt.ResearchProofs.JitFrontiers.DemandSSA.Goal_AbsorbAnd_DropGuardInFilter
