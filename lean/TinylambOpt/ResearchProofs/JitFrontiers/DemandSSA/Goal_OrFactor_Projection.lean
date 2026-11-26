import TinylambOpt.ResearchProofs.JitFrontiers.DemandSSA.Common_DemandSSA

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.DemandSSA.Goal_OrFactor_Projection

open TinylambOpt.ResearchGoals.JitFrontiers.DemandSSA
open TinylambOpt.ResearchProofs.JitFrontiers.DemandSSA.Common

theorem refute : ¬ OrFactor.Projection.Goal := by
  intro h
  have hb := h [.null, .yes, .fault 7] (by native_decide)
  have hbad : ¬ ((run [.null, .yes, .fault 7] OrFactor.rule.dst).1 = (run [.null, .yes, .fault 7] OrFactor.rule.src).1) := by native_decide
  exact hbad hb

end TinylambOpt.ResearchProofs.JitFrontiers.DemandSSA.Goal_OrFactor_Projection
