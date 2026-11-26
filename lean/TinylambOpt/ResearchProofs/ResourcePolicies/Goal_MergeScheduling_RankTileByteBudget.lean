import TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false
set_option linter.deprecated false
set_option linter.unusedVariables false
set_option linter.unusedSectionVars false

/-! Proof/counterexample for `TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling.RankTileByteBudget.Goal`. The theorem below certifies the resource-policy claim (or refutes the strong hypothesis) in the model defined by that module, preserving the modeled result. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_MergeScheduling_RankTileByteBudget

open TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling

theorem refute : ¬ RankTileByteBudget.Goal := by
  intro h
  have hsum : ([10, 0, 0] : List Nat).sum ≤ [10, 0, 0].length * 4 := by decide
  have := h [10, 0, 0] 1 4 hsum 0
  simp at this

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_MergeScheduling_RankTileByteBudget
