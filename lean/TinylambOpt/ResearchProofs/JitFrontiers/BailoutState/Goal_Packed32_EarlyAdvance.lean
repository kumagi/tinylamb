import TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Common_BailoutState

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Goal_Packed32_EarlyAdvance

open TinylambOpt.ResearchGoals.JitFrontiers.BailoutState
open TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Common

theorem refute : ¬ Packed32.EarlyAdvance.Goal := by
  exact earlyAdvance_refuted Packed32.policy (0, some 1) (by decide) (by decide)

end TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Goal_Packed32_EarlyAdvance
