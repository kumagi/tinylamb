import TinylambOpt.ResearchGoals.EngineBoundaries.RadixBudget

/-!
Partitioning for a spillable SQL operator must satisfy the modeled size or resource
bound for its partitions.

Proved

- `LeafCount` — Binary partitioning bounds the number of concurrently retained leaf
  descriptors.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.EngineBoundaries.Goal_RadixBudget_LeafCount

open TinylambOpt.ResearchGoals.EngineBoundaries.RadixBudget

theorem sound : LeafCount.Goal := by
  intro b d rs
  induction d generalizing rs with
  | zero => simp [leaves]
  | succ d ih =>
      have hpow : 1 ≤ 2 ^ d := Nat.one_le_pow d 2 (by decide)
      by_cases hsmall : size rs ≤ b
      · simp [leaves, hsmall, Nat.pow_succ]
        omega
      · simp only [leaves, hsmall]
        have side : ∀ side : Bool,
            (if bucket d side rs = [] then [] else
              if b < size (bucket d side rs) ∧ stagnant (size (bucket d side rs)) (size rs) then
                [⟨bucket d side rs, true⟩]
              else leaves b d (bucket d side rs)).length ≤ 2 ^ d := by
          intro side
          by_cases he : bucket d side rs = []
          · simp [he]
          · by_cases hs : b < size (bucket d side rs) ∧
                stagnant (size (bucket d side rs)) (size rs) = true
            · simp [he, hs, hpow]
            · simpa [he, hs] using ih (bucket d side rs)
        have ht := side true
        have hf := side false
        simp
        omega

end TinylambOpt.ResearchProofs.EngineBoundaries.Goal_RadixBudget_LeafCount
