import TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling
set_option linter.unnecessarySimpa false
set_option linter.deprecated false
set_option linter.unusedVariables false
set_option linter.unusedSectionVars false

/-! Proof/counterexample for `TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling.MorselEvaluationCharge.Goal`. The theorem below certifies the resource-policy claim (or refutes the strong hypothesis) in the model defined by that module, preserving the modeled result. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_PredicateScheduling_MorselEvaluationCharge
open TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling
open TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling.MorselEvaluationCharge
theorem sound : Goal := by
  intro ts a b
  induction ts generalizing a b with
  | nil => simp [work]
  | cons t ts ih =>
    simp [work, List.map_append, List.sum_append, List.filter_append, ih]
    omega
end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_PredicateScheduling_MorselEvaluationCharge
