import TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates

/-!
# Goal_PartitionedAggregateStates_DemographicPriceIntervalDispatch

Registers the proof of the DemographicPriceIntervalDispatch goal in PartitionedAggregateStates: the modeled rewrite
preserves the exact result, so SQL execution may apply it.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.QueryRewrites.Goal_PartitionedAggregateStates_DemographicPriceIntervalDispatch
open TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates.DemographicPriceIntervalDispatch
theorem sound : Goal := by
  intro rules k price
  dsimp [Goal, TinylambOpt.ResearchGoals.QueryRewrites.forKey]
  induction rules with
  | nil => simp
  | cons r rs ih =>
      obtain ⟨c, l, h⟩ := r
      simp only [List.filter_cons, List.map_cons, List.any_cons]
      by_cases hk : c == k
      · simp [hk, ih]
      · simp [hk, ih]
end TinylambOpt.ResearchProofs.QueryRewrites.Goal_PartitionedAggregateStates_DemographicPriceIntervalDispatch
