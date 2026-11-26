import TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Common_BailoutState

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Goal_LastValue_LastWins

open TinylambOpt.ResearchGoals.JitFrontiers.BailoutState
open TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Common


theorem sound : LastValue.LastWins.Goal := by
  intro s k a b
  simp [refRun, step, LastValue.policy]

end TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Goal_LastValue_LastWins
