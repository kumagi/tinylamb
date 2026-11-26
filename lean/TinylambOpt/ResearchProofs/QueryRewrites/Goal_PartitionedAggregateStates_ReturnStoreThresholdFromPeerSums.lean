import TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates

/-!
# Goal_PartitionedAggregateStates_ReturnStoreThresholdFromPeerSums

Registers the proof of the ReturnStoreThresholdFromPeerSums goal in PartitionedAggregateStates: the modeled rewrite
preserves the exact result, so SQL execution may apply it.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.QueryRewrites.Goal_PartitionedAggregateStates_ReturnStoreThresholdFromPeerSums
open TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates.ReturnStoreThresholdFromPeerSums
theorem sound : Goal := by
  intro peers own
  dsimp [Goal]
  -- both sides are the same inequality after algebra
  constructor <;> intro h <;> {
    -- 5*((n+1))*own = 5*n*own + 5*own, 6*(own+s)=6*own+6*s
    -- so 5*n*own + 5*own > 6*own + 6*s <-> 5*n*own - own > 6*s <-> (5*n-1)*own > 6*s
    have h' := h
    simp only [Int.add_mul, Int.mul_add, Int.sub_mul, Int.one_mul] at h' ⊢
    omega
  }
end TinylambOpt.ResearchProofs.QueryRewrites.Goal_PartitionedAggregateStates_ReturnStoreThresholdFromPeerSums
