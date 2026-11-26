import TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Common_BailoutState

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Goal_BucketCount_SingleBucket

open TinylambOpt.ResearchGoals.JitFrontiers.BailoutState
open TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Common


theorem sound : BucketCount.SingleBucket.Goal := by
  intro k
  have h1 : (64 * k) / 64 = k := by omega
  have h2 : (64 * k + 63) / 64 = k := by omega
  have hs1 : step BucketCount.policy [] (64 * k, none) = [(k, 1)] := by
    simp [step, BucketCount.policy, h1]
  have hc1 : canCommit BucketCount.policy 1 [] (64 * k, none) = true := by
    simp [canCommit, BucketCount.policy]
  have hc2 : canCommit BucketCount.policy 1 [(k, 1)] (64 * k + 63, none) = true := by
    simp [canCommit, BucketCount.policy, h2]
  have hs2 : step BucketCount.policy [(k, 1)] (64 * k + 63, none) = [(k, 2)] := by
    simp [step, BucketCount.policy, h2]
  simp [run, drive, hc1, hs1, hc2, hs2]

end TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Goal_BucketCount_SingleBucket
