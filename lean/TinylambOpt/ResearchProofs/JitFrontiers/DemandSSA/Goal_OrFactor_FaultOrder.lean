import TinylambOpt.ResearchProofs.JitFrontiers.DemandSSA.Common_DemandSSA

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.DemandSSA.Goal_OrFactor_FaultOrder

open TinylambOpt.ResearchGoals.JitFrontiers.DemandSSA
open TinylambOpt.ResearchProofs.JitFrontiers.DemandSSA.Common

theorem refute : ¬ OrFactor.FaultOrder.Goal := by
  intro h
  have hb := h [[.yes,.yes,.yes], [.null,.yes,.fault 7]]
  have hbad : ¬ (rowsFail (dispatch OrFactor.rule) [[.yes,.yes,.yes], [.null,.yes,.fault 7]] = rowsFail (fun e => run e OrFactor.rule.src) [[.yes,.yes,.yes], [.null,.yes,.fault 7]]) := by native_decide
  exact hbad hb

end TinylambOpt.ResearchProofs.JitFrontiers.DemandSSA.Goal_OrFactor_FaultOrder
