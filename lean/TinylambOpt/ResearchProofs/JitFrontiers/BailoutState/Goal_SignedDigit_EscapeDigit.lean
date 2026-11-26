import TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Common_BailoutState

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Goal_SignedDigit_EscapeDigit

open TinylambOpt.ResearchGoals.JitFrontiers.BailoutState
open TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Common


theorem sound : SignedDigit.EscapeDigit.Goal := by
  intro k
  simp [run, drive, canCommit, SignedDigit.policy]

end TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Goal_SignedDigit_EscapeDigit
