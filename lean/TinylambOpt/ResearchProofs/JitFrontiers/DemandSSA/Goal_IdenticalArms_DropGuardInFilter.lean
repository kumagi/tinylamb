import TinylambOpt.ResearchProofs.JitFrontiers.DemandSSA.Common_DemandSSA

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.DemandSSA.Goal_IdenticalArms_DropGuardInFilter

open TinylambOpt.ResearchGoals.JitFrontiers.DemandSSA
open TinylambOpt.ResearchProofs.JitFrontiers.DemandSSA.Common

theorem refute : ¬ IdenticalArms.DropGuardInFilter.Goal := by
  intro h
  have hb := h [.fault 7, .yes]
  have hbad : ¬ (accepted (run [.fault 7, .yes] IdenticalArms.rule.dst).1 = accepted (run [.fault 7, .yes] IdenticalArms.rule.src).1) := by native_decide
  exact hbad hb

end TinylambOpt.ResearchProofs.JitFrontiers.DemandSSA.Goal_IdenticalArms_DropGuardInFilter
