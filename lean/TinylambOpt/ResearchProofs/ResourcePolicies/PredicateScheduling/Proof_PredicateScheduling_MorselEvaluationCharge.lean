import TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace Nov26.Proof_PredicateScheduling_MorselEvaluationCharge
open TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling
open TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling.MorselEvaluationCharge
theorem sound : Goal := by
  intro ts a b
  induction ts generalizing a b with
  | nil => simp [work]
  | cons t ts ih =>
    simp [work, List.map_append, List.sum_append, List.filter_append, ih]
    omega
end Nov26.Proof_PredicateScheduling_MorselEvaluationCharge
