import TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling
set_option linter.unnecessarySimpa false
set_option linter.deprecated false
set_option linter.unusedVariables false
set_option linter.unusedSectionVars false

/-! Proof/counterexample for `TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling.FallibleFilterCommutation.Goal`. The theorem below certifies the resource-policy claim (or refutes the strong hypothesis) in the model defined by that module, preserving the modeled result. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_PredicateScheduling_FallibleFilterCommutation
open TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling
open TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling.FallibleFilterCommutation

theorem sound : Goal := by
  intro a b rest rows h
  apply List.map_congr_left
  intro r hr
  simp only [evaluate]
  cases ha : a r <;> cases hb : b r <;> simp [ha, hb] at h ⊢
  all_goals try rfl
  all_goals try (have := h r hr (by simp [ha]) (by simp [hb]); simp_all)
end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_PredicateScheduling_FallibleFilterCommutation
