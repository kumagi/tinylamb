import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

/-!
Scan pruning can exploit derived bounds only when every matching row remains covered by
the block summary.

Proved

- `QuadraticGap.SoundInterval` — Bound a computed group or predicate key from
  independent column min/max.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_QuadraticGap_SoundInterval

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.QuadraticGap

/-- Squaring preserves the interval: the truncated absolute difference of two
points in independent intervals lies between the interval separation and the
interval diameter. -/
theorem sound_SoundInterval : QuadraticGap.SoundInterval.Goal := by
  intro t a b x y hax hby
  rcases a with ⟨alo, ahi⟩
  rcases b with ⟨blo, bhi⟩
  simp only [inside] at hax hby
  simp only [kernel, gapMin, gapMax, gap]
  have hgapMin : max (alo - bhi) (blo - ahi) ≤ (x - y) + (y - x) := by
    apply Nat.max_le.mpr
    constructor <;> omega
  have hgapMax : (x - y) + (y - x) ≤ max (ahi - blo) (bhi - alo) := by
    by_cases hyx : y ≤ x
    · have hzero : y - x = 0 := Nat.sub_eq_zero_of_le hyx
      rw [hzero, Nat.add_zero]
      exact Nat.le_trans (by omega : x - y ≤ ahi - blo) (Nat.le_max_left _ _)
    · have hxy : x ≤ y := by omega
      have hzero : x - y = 0 := Nat.sub_eq_zero_of_le hxy
      rw [hzero, Nat.zero_add]
      exact Nat.le_trans (by omega : y - x ≤ bhi - alo) (Nat.le_max_right _ _)
  constructor
  · exact Nat.add_le_add_right (Nat.pow_le_pow_left hgapMin 2) t
  · exact Nat.add_le_add_right (Nat.pow_le_pow_left hgapMax 2) t

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_QuadraticGap_SoundInterval
