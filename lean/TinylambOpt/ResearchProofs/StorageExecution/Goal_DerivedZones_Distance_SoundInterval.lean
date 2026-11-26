import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

/-!
Scan pruning can exploit derived bounds only when every matching row remains covered by
the block summary.

Proved

- `Distance.SoundInterval` — Bound a computed group or predicate key from independent
  column min/max.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_Distance_SoundInterval

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.Distance

-- Exhaustive check on a small grid (all bounds with 0 <= lo <= hi <= 6 and
-- 0 <= x,y <= 6): every hypothesis-satisfying input satisfies the interval.
private def smallBounds : List Bounds :=
  (List.range 7).flatMap fun lo =>
    ((List.range 7).filter (fun hi => decide (lo ≤ hi))).map fun hi => ⟨lo, hi⟩

private def counterexamples : List (Bounds × Bounds × Nat × Nat) :=
  smallBounds.flatMap fun a =>
  smallBounds.flatMap fun b =>
  (List.range 7).flatMap fun x =>
  (List.range 7).filterMap fun y =>
    if decide (a.lo ≤ x ∧ x ≤ a.hi ∧ b.lo ≤ y ∧ y ≤ b.hi ∧
        ¬ ((kernel.bounds 0 a b).lo ≤ kernel.score 0 (x, y) ∧
           kernel.score 0 (x, y) ≤ (kernel.bounds 0 a b).hi)) then
      some (a, b, x, y)
    else none

#eval counterexamples.length

/-- The truncated absolute difference of two points in independent intervals
is at least the separation of the intervals and at most their diameter. -/
theorem sound_SoundInterval : Distance.SoundInterval.Goal := by
  intro t a b x y hax hby
  rcases a with ⟨alo, ahi⟩
  rcases b with ⟨blo, bhi⟩
  simp only [inside] at hax hby
  simp only [kernel, gapMin, gapMax, gap]
  constructor
  · apply Nat.max_le.mpr
    constructor <;> omega
  · by_cases hyx : y ≤ x
    · have hzero : y - x = 0 := Nat.sub_eq_zero_of_le hyx
      rw [hzero, Nat.add_zero]
      exact Nat.le_trans (by omega : x - y ≤ ahi - blo) (Nat.le_max_left _ _)
    · have hxy : x ≤ y := by omega
      have hzero : x - y = 0 := Nat.sub_eq_zero_of_le hxy
      rw [hzero, Nat.zero_add]
      exact Nat.le_trans (by omega : y - x ≤ bhi - alo) (Nat.le_max_right _ _)

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_Distance_SoundInterval
