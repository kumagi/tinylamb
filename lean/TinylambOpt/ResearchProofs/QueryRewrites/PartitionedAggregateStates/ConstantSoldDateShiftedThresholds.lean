import TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates

/-!
# ConstantSoldDateShiftedThresholds
-/

set_option maxHeartbeats 0

namespace TinylambOpt.ResearchProofs.QueryRewrites.PartitionedAggregateStates.ConstantSoldDateShiftedThresholds
open TinylambOpt.ResearchGoals.QueryRewrites

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates.ConstantSoldDateShiftedThresholds.Goal := by
  intro sold shipped
  dsimp only [TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates.ConstantSoldDateShiftedThresholds.Goal, bucket]
  simp only [List.map, List.countP_cons, List.countP_nil, decide_eq_true_eq]
  -- normalize to Nat arithmetic on the conditions
  have h1 : (sold + 30 < shipped) ↔ (30 < shipped - sold) := by omega
  have h2 : (sold + 60 < shipped) ↔ (60 < shipped - sold) := by omega
  have h3 : (sold + 90 < shipped) ↔ (90 < shipped - sold) := by omega
  have h4 : (sold + 120 < shipped) ↔ (120 < shipped - sold) := by omega
  simp [h1, h2, h3, h4]

#print axioms TinylambOpt.ResearchProofs.QueryRewrites.PartitionedAggregateStates.ConstantSoldDateShiftedThresholds.sound
