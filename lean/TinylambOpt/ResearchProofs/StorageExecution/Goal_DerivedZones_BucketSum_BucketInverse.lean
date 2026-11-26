import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_BucketSum_BucketInverse

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.BucketSum

theorem sound_BucketInverse : BucketSum.BucketInverse.Goal := by
  intro t x y k
  simp only [kernel]
  constructor
  · intro h
    constructor
    · rw [← h]
      exact Nat.div_mul_le_self (x + y) (t + 1)
    · have hlt : (x + y) / (t + 1) < k + 1 := by
        rw [h]
        exact Nat.lt_succ_self k
      exact (Nat.div_lt_iff_lt_mul (by omega)).mp hlt
  · intro h
    exact Nat.div_eq_of_lt_le h.1 h.2

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_BucketSum_BucketInverse
