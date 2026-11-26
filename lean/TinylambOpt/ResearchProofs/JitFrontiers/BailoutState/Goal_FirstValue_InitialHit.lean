import TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Common_BailoutState

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Goal_FirstValue_InitialHit

open TinylambOpt.ResearchGoals.JitFrontiers.BailoutState
open TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Common


theorem sound : FirstValue.InitialHit.Goal := by
  intro k x y
  simp [step, FirstValue.policy]

end TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Goal_FirstValue_InitialHit
