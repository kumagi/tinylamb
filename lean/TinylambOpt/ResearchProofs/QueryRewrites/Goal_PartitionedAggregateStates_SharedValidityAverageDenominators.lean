import TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates

/-!
# Goal_PartitionedAggregateStates_SharedValidityAverageDenominators

Registers the proof of the SharedValidityAverageDenominators goal in PartitionedAggregateStates: the modeled rewrite
preserves the exact result, so SQL execution may apply it.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.QueryRewrites.Goal_PartitionedAggregateStates_SharedValidityAverageDenominators
open TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates.SharedValidityAverageDenominators
theorem sound : Goal := by
  intro rs hmask j
  congr 1
  have hlen : (rs.filterMap (fun r => r j)).length = (rs.filterMap (fun r => r 0)).length := by
    induction rs with
    | nil => simp
    | cons r rs ih =>
        have hj := hmask r (by simp) j
        have ih' := ih (fun r' hr' => hmask r' (by simp [hr']))
        cases h0 : r 0 <;> cases hj2 : r j <;> simp [List.filterMap_cons, h0, hj2] at hj ⊢ <;> try simp_all
        all_goals try omega
        all_goals try (simp [ih'])
  exact hlen
end TinylambOpt.ResearchProofs.QueryRewrites.Goal_PartitionedAggregateStates_SharedValidityAverageDenominators
