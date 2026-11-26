import TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates

namespace TinylambOpt.ResearchProofs.Batch5.QueryRewrites.PartitionedAggregateStates.Goal_ReturnStoreThresholdFromPeerSums

open TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates


/-!
Soundness of the peer-sum form of the six-fifths return-store HAVING guard.
Both sides are the same exact integer linear inequality after expansion, so the
rewritten state (peer subtotal plus valid-peer count) selects exactly the same rows.
-/

set_option maxHeartbeats 0

open TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates

theorem sound : ReturnStoreThresholdFromPeerSums.Goal := by
  intro peers own
  dsimp
  rw [Int.mul_add, Int.add_mul, Int.mul_assoc]
  simp only [Int.mul_one]
  rw [Int.mul_add, Int.sub_mul, Int.mul_assoc]
  omega
end TinylambOpt.ResearchProofs.Batch5.QueryRewrites.PartitionedAggregateStates.Goal_ReturnStoreThresholdFromPeerSums
