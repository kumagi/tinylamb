import TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling

namespace TinylambOpt.ResearchProofs.ResourcePolicies.MergeScheduling.Goal_WeightGuidedRotation

open TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling


/-! Soundness of `WeightGuidedRotation`: choosing the smaller outer run for the
    final merge is exactly the write-minimising choice for a three-run tree. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

open TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling.WeightGuidedRotation.Goal := by
  intro a b c
  simp only [TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling.WeightGuidedRotation.Goal,
    writes, size]
  omega
end TinylambOpt.ResearchProofs.ResourcePolicies.MergeScheduling.Goal_WeightGuidedRotation
