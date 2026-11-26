import TinylambOpt.ResearchGoals.EngineBoundaries.RadixBudget

/-!
Partitioning for a spillable SQL operator must satisfy the modeled size or resource
bound for its partitions.

Proved

- `UniqueHeavyChild` — At most one child can exceed the 80% threshold in a binary pass.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.EngineBoundaries.Goal_RadixBudget_UniqueHeavyChild

open TinylambOpt.ResearchGoals.EngineBoundaries.RadixBudget

private theorem bucket_split (bit : Nat) (rs : Batch) :
    size (bucket bit true rs) + size (bucket bit false rs) = size rs := by
  induction rs with
  | nil => rfl
  | cons r rs ih =>
      simp [bucket, size] at ih ⊢
      by_cases ht : r.hash.testBit bit
      · simp [ht] at ih ⊢
        omega
      · have hf : (!r.hash.testBit bit) = true := by simp [ht]
        simp [ht, hf] at ih ⊢
        omega

theorem sound : UniqueHeavyChild.Goal := by
  intro rs bit h
  have hsum : size (bucket bit true rs) + size (bucket bit false rs) = size rs := bucket_split bit rs
  simp [stagnant] at h
  omega

end TinylambOpt.ResearchProofs.EngineBoundaries.Goal_RadixBudget_UniqueHeavyChild
