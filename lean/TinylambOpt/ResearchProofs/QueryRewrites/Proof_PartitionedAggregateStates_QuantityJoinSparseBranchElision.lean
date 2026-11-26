import TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates

namespace TinylambOpt.ResearchProofs.QueryRewrites.PartitionedAggregateStates.Goal_QuantityJoinSparseBranchElision

open TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates


/-! Tickets whose catalog slice is empty contribute no joined rows, so they can
be removed before the three quantity columns are expanded. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates.QuantityJoinSparseBranchElision.Goal := by
  intro tickets catalog
  induction tickets with
  | nil => rfl
  | cons t ts ih =>
      simp only [List.filter_cons, List.flatMap_cons]
      by_cases h : (catalog t.1).isEmpty
      · have hnil : catalog t.1 = [] := by simpa using h
        simp [h, hnil, ih]
      · simp [h, ih]
end TinylambOpt.ResearchProofs.QueryRewrites.PartitionedAggregateStates.Goal_QuantityJoinSparseBranchElision
