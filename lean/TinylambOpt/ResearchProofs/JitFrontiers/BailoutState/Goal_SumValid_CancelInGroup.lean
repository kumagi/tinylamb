import TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Common_BailoutState

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Goal_SumValid_CancelInGroup

open TinylambOpt.ResearchGoals.JitFrontiers.BailoutState
open TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Common


theorem sound : SumValid.CancelInGroup.Goal := by
  intro k x
  simp [refRun, step, SumValid.policy]
  omega

end TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Goal_SumValid_CancelInGroup
