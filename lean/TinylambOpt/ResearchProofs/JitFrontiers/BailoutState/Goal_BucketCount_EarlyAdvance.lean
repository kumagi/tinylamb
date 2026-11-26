import TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Common_BailoutState

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Goal_BucketCount_EarlyAdvance

open TinylambOpt.ResearchGoals.JitFrontiers.BailoutState
open TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Common

theorem refute : ¬ BucketCount.EarlyAdvance.Goal := by
  exact earlyAdvance_refuted BucketCount.policy (0, none) (by decide) (by decide)

end TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Goal_BucketCount_EarlyAdvance
