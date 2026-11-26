import TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false
set_option linter.deprecated false
set_option linter.unusedVariables false
set_option linter.unusedSectionVars false

/-! Proof/counterexample for `TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling.WeightGuidedRotation.Goal`. The theorem below certifies the resource-policy claim (or refutes the strong hypothesis) in the model defined by that module, preserving the modeled result. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_MergeScheduling_WeightGuidedRotation

open TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling

theorem goal : WeightGuidedRotation.Goal := by
  intro a b c
  simp only [WeightGuidedRotation.Goal, writes, size]
  omega

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_MergeScheduling_WeightGuidedRotation
