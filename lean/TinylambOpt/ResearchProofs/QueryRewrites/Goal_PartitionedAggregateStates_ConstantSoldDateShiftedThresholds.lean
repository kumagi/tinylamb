import TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates

/-!
# Goal_PartitionedAggregateStates_ConstantSoldDateShiftedThresholds

Registers the proof of the ConstantSoldDateShiftedThresholds goal in PartitionedAggregateStates: the modeled rewrite
preserves the exact result, so SQL execution may apply it.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.QueryRewrites.Goal_PartitionedAggregateStates_ConstantSoldDateShiftedThresholds
open TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates.ConstantSoldDateShiftedThresholds
theorem sound : Goal := by
  intro sold shipped
  dsimp [Goal, TinylambOpt.ResearchGoals.QueryRewrites.bucket]
  simp only [List.map, List.countP_cons, List.countP_nil]
  have h30 : (30 < shipped - sold) ↔ (sold + 30 < shipped) := by constructor <;> intro h <;> omega
  have h60 : (60 < shipped - sold) ↔ (sold + 60 < shipped) := by constructor <;> intro h <;> omega
  have h90 : (90 < shipped - sold) ↔ (sold + 90 < shipped) := by constructor <;> intro h <;> omega
  have h120 : (120 < shipped - sold) ↔ (sold + 120 < shipped) := by constructor <;> intro h <;> omega
  simp [h30, h60, h90, h120]
end TinylambOpt.ResearchProofs.QueryRewrites.Goal_PartitionedAggregateStates_ConstantSoldDateShiftedThresholds
