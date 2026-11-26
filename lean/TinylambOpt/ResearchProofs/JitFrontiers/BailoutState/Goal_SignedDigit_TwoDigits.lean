import TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Common_BailoutState

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Goal_SignedDigit_TwoDigits

open TinylambOpt.ResearchGoals.JitFrontiers.BailoutState
open TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Common


theorem sound : SignedDigit.TwoDigits.Goal := by
  intro k a b
  simp [refRun, step, SignedDigit.policy]

end TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Goal_SignedDigit_TwoDigits
