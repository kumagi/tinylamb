import TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false
set_option linter.deprecated false
set_option linter.unusedVariables false
set_option linter.unusedSectionVars false

/-! Proof/counterexample for `TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling.RewriteMinimumAlsoMinimizesPeak.Goal`. The theorem below certifies the resource-policy claim (or refutes the strong hypothesis) in the model defined by that module, preserving the modeled result. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_MergeScheduling_RewriteMinimumAlsoMinimizesPeak

open TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling

theorem refute : ¬ RewriteMinimumAlsoMinimizesPeak.Goal := by
  intro h
  let a : Run := .source 1
  let b : Run := .source 1
  let c : Run := .merge (.source 1) (.source 1)
  have hw : writes (.merge (.merge a b) c) ≤ writes (.merge a (.merge b c)) := by native_decide
  have hp := h a b c hw
  simp [a, b, c, peak, temp, size] at hp

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_MergeScheduling_RewriteMinimumAlsoMinimizesPeak
