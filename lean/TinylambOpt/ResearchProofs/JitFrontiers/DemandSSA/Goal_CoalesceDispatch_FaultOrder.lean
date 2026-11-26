import TinylambOpt.ResearchProofs.JitFrontiers.DemandSSA.Common_DemandSSA

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.DemandSSA.Goal_CoalesceDispatch_FaultOrder

open TinylambOpt.ResearchGoals.JitFrontiers.DemandSSA
open TinylambOpt.ResearchProofs.JitFrontiers.DemandSSA.Common

theorem sound : CoalesceDispatch.FaultOrder.Goal := by
  apply faultOrder_of_pointwise
  intro e s
  cases h0 : slot e 0 <;> cases h1 : slot e 1 <;> cases h2 : slot e 2 <;>
    simp [slot] at h0 h1 h2 <;>
    simp [slot, run, after, dispatch, bne, instBEqV.beq, CoalesceDispatch.rule, usable, present, h0, h1, h2] at ⊢

end TinylambOpt.ResearchProofs.JitFrontiers.DemandSSA.Goal_CoalesceDispatch_FaultOrder
