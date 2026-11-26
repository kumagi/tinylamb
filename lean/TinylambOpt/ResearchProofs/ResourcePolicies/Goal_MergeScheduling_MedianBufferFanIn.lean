import TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false
set_option linter.deprecated false
set_option linter.unusedVariables false
set_option linter.unusedSectionVars false

/-! Proof/counterexample for `TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling.MedianBufferFanIn.Goal`. The theorem below certifies the resource-policy claim (or refutes the strong hypothesis) in the model defined by that module, preserving the modeled result. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_MergeScheduling_MedianBufferFanIn

open TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling

theorem refute : ¬ MedianBufferFanIn.Goal := by
  intro h
  let bs : List Nat := [1, 1, 100]
  have hne : bs ≠ [] := by decide
  have hfd : bs.length + 1 ≤ 4 := by decide
  have hmem : bs.length * ((bs.mergeSort (· ≤ ·))[bs.length / 2]?.getD 0) + 0 ≤ 3 := by native_decide
  have hfan := h bs 3 4 0 hne hfd hmem
  have hsum : ¬ (bs.sum + 0 ≤ 3) := by decide
  exact hsum hfan.2

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_MergeScheduling_MedianBufferFanIn
