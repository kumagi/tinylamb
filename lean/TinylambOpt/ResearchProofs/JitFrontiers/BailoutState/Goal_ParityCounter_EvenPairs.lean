import TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Common_BailoutState

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Goal_ParityCounter_EvenPairs

open TinylambOpt.ResearchGoals.JitFrontiers.BailoutState
open TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Common


theorem sound : ParityCounter.EvenPairs.Goal := by
  intro k v
  simp [refRun, step, ParityCounter.policy]

end TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Goal_ParityCounter_EvenPairs
