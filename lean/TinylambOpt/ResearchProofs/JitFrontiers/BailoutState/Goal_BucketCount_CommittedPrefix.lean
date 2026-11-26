import TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Common_BailoutState

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Goal_BucketCount_CommittedPrefix

open TinylambOpt.ResearchGoals.JitFrontiers.BailoutState
open TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Common

theorem sound : BucketCount.CommittedPrefix.Goal := by
  exact committedPrefix_of BucketCount.policy

end TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Goal_BucketCount_CommittedPrefix
