import TinylambOpt.ResearchGoals.StorageExecution.PostingJoins

/-!
Posting-list execution can use key and interval structure to reduce the rows it examines
while preserving the modeled join or count.

Proved

- `SameBucket.PruneExtent` — Elide a posting list when its stored endpoints miss every
  interval.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_SameBucket_PruneExtent

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.SameBucket

/-- If the stored bucket interval misses `[lo, hi)`, no key in that extent
    can fall into the same bucket as the probe key. -/
theorem sound_PruneExtent :
    TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.SameBucket.PruneExtent.Goal := by
  intro u t x ys lo hi hv hys hspan
  rw [List.filter_eq_nil_iff]
  intro y hy
  have hyu : y < u := hv.2 y hy
  have hlo : lo ≤ y := (hys y hy).1
  have hhi : y < hi := (hys y hy).2
  cases htest : search.test t x y with
  | false => simp
  | true =>
      have hxw : x / (t + 1) = y / (t + 1) := by
        simpa [search, beq_iff_eq] using htest
      let a := x / (t + 1) * (t + 1)
      let b := min u (a + t + 1)
      have hya : a ≤ y := by
        dsimp [a]
        rw [hxw]
        exact Nat.div_mul_le_self y (t + 1)
      have hyb : y < b := by
        have hlt : y < y / (t + 1) * (t + 1) + (t + 1) :=
          Nat.lt_div_mul_add (by omega : 0 < t + 1)
        rw [← hxw] at hlt
        dsimp [b, a]
        exact Nat.lt_min.mpr ⟨hyu, hlt⟩
      have hab : a < b := by omega
      have hab' : x / (t + 1) * (t + 1) <
          min u (x / (t + 1) * (t + 1) + t + 1) := by
        simpa [a, b] using hab
      have hspans : search.spans u t x = [(a, b)] := by
        simp [search, a, b,
          TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional, hab']
      have hp := hspan (a, b) (by simp [hspans])
      rcases hp with hp | hp <;> omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_SameBucket_PruneExtent
