import TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Common_BailoutState

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Goal_LastValue_DistinctOrder

open TinylambOpt.ResearchGoals.JitFrontiers.BailoutState
open TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Common


theorem sound : LastValue.DistinctOrder.Goal := by
  intro k a b hab
  simp [refRun, step, LastValue.policy]
  intro h
  exact hab h.symm

end TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Goal_LastValue_DistinctOrder
