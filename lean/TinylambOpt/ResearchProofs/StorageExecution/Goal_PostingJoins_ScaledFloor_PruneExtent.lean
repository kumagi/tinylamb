import TinylambOpt.ResearchGoals.StorageExecution.PostingJoins

/-!
Posting-list execution can use key and interval structure to reduce the rows it examines
while preserving the modeled join or count.

Proved

- `ScaledFloor.PruneExtent` — Elide a posting list when its stored endpoints miss every
  interval.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_ScaledFloor_PruneExtent

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.ScaledFloor

/-- The stored interval `[t*(x+1), min u ((t+1)*(x+1)))` contains every
    right key whose quotient by `x+1` is exactly `t`.  Hence an extent that
    misses this interval cannot contain a matching key. -/
theorem sound_PruneExtent : ScaledFloor.PruneExtent.Goal := by
  intro u t x ys lo hi hv hys hspan
  rw [List.filter_eq_nil_iff]
  intro y hy
  have hyu : y < u := hv.2 y hy
  cases htest : search.test t x y with
  | false => simp [htest]
  | true =>
      have hdiv : y / (x + 1) = t := by
        simpa [search, beq_iff_eq] using htest
      let a := t * (x + 1)
      let b := min u ((t + 1) * (x + 1))
      have ha_le_y : a ≤ y := by
        simpa [a, hdiv] using Nat.div_mul_le_self y (x + 1)
      have hy_lt_c : y < (t + 1) * (x + 1) := by
        have hlt : y / (x + 1) < t + 1 := by omega
        exact (Nat.div_lt_iff_lt_mul (by omega : 0 < x + 1)).mp hlt
      have hy_lt_b : y < b := by
        dsimp [b]
        exact (Nat.lt_min).2 ⟨hyu, hy_lt_c⟩
      by_cases hab : a < b
      · have hp := hspan (a, b) (by
          simp [search, TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional, a, b, hab])
        have hlo : lo ≤ y := (hys y hy).1
        have hhi : y < hi := (hys y hy).2
        rcases hp with hpb | hpa <;> omega
      · omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_ScaledFloor_PruneExtent
