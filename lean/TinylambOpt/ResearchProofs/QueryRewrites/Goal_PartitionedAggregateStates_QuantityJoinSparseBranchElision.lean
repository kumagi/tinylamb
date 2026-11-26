import TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates

/-!
# Goal_PartitionedAggregateStates_QuantityJoinSparseBranchElision

Registers the proof of the QuantityJoinSparseBranchElision goal in PartitionedAggregateStates: the modeled rewrite
preserves the exact result, so SQL execution may apply it.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.QueryRewrites.Goal_PartitionedAggregateStates_QuantityJoinSparseBranchElision
open TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates.QuantityJoinSparseBranchElision
theorem sound : Goal := by
  intro tickets catalog
  dsimp [Goal]
  induction tickets with
  | nil => simp
  | cons t ts ih =>
      by_cases h : (catalog t.1).isEmpty = true
      · have hempty : catalog t.1 = [] := by
          cases hc : catalog t.1 <;> simp_all
        simp [hempty, ih]
      · have hf : (!(catalog t.1).isEmpty) = true := by
          cases hc : (catalog t.1).isEmpty <;> simp_all
        simp [hf, ih]
end TinylambOpt.ResearchProofs.QueryRewrites.Goal_PartitionedAggregateStates_QuantityJoinSparseBranchElision
