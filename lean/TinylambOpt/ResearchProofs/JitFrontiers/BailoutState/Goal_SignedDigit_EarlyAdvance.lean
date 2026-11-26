import TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Common_BailoutState

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Goal_SignedDigit_EarlyAdvance

open TinylambOpt.ResearchGoals.JitFrontiers.BailoutState
open TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Common

theorem refute : ¬ SignedDigit.EarlyAdvance.Goal := by
  exact earlyAdvance_refuted SignedDigit.policy (0, some 1) (by decide) (by decide)

end TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Goal_SignedDigit_EarlyAdvance
