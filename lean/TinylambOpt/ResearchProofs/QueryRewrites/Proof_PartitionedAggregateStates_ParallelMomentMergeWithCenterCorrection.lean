import TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates

namespace TinylambOpt.ResearchProofs.QueryRewrites.PartitionedAggregateStates.Goal_ParallelMomentMergeWithCenterCorrection

open TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates


/-! Two nonempty sample blocks may merge their variance numerators before
materializing the concatenated rows. After expanding the append, sum, length,
and exact squared-moment states, the claimed formula is a polynomial identity
in the two sums and two square sums; no division or rounding enters this
numerator-level rewrite. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

open TinylambOpt.ResearchGoals.QueryRewrites

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates.ParallelMomentMergeWithCenterCorrection.Goal := by
  intro a b ha hb
  simp [total, List.map_append, List.sum_append]
  grind
end TinylambOpt.ResearchProofs.QueryRewrites.PartitionedAggregateStates.Goal_ParallelMomentMergeWithCenterCorrection
