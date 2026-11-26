import TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates

namespace TinylambOpt.ResearchProofs.Batch5.QueryRewrites.PartitionedAggregateStates.Goal_DemographicPriceIntervalDispatch

open TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates


/-! The matching demographic class selects which inclusive price intervals are
tested. `forKey` is a filter followed by a projection, so an existential over
that projection is exactly an existential over the original rule list with the
class equality conjoined. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates.DemographicPriceIntervalDispatch.Goal := by
  intro rules k price
  induction rules with
  | nil => simp [TinylambOpt.ResearchGoals.QueryRewrites.forKey]
  | cons r rs ih =>
      by_cases hk : r.1 == k
      · simp [TinylambOpt.ResearchGoals.QueryRewrites.forKey, hk, ih, Bool.and_assoc]
      · simp [TinylambOpt.ResearchGoals.QueryRewrites.forKey, hk, ih, Bool.and_assoc]
end TinylambOpt.ResearchProofs.Batch5.QueryRewrites.PartitionedAggregateStates.Goal_DemographicPriceIntervalDispatch
