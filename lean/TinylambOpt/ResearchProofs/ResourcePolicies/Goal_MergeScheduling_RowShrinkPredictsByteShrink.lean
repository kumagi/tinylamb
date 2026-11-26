import TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false
set_option linter.deprecated false
set_option linter.unusedVariables false
set_option linter.unusedSectionVars false

/-! Proof/counterexample for `TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling.RowShrinkPredictsByteShrink.Goal`. The theorem below certifies the resource-policy claim (or refutes the strong hypothesis) in the model defined by that module, preserving the modeled result. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_MergeScheduling_RowShrinkPredictsByteShrink

open TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling

theorem refute : ¬ RowShrinkPredictsByteShrink.Goal := by
  intro h
  have hrows : 5 * ([100] : List Nat).length ≤ 4 * ([100] ++ [1,1,1,1,1]).length := by decide
  have := h [100] [1,1,1,1,1] hrows
  simp at this

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_MergeScheduling_RowShrinkPredictsByteShrink
