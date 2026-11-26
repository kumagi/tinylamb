import TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Common_BailoutState

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Goal_LastValue_IncreaseCapacity

open TinylambOpt.ResearchGoals.JitFrontiers.BailoutState
open TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Common

theorem sound : LastValue.IncreaseCapacity.Goal := by
  exact increaseCapacity_of LastValue.policy

end TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Goal_LastValue_IncreaseCapacity
