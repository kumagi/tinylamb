import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

/-!
Scan pruning can exploit derived bounds only when every matching row remains covered by
the block summary.

Proved

- `QuadraticGap.TakeAll` — Eliminate the scalar predicate once the entire score
  interval fits.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_QuadraticGap_TakeAll

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.QuadraticGap

-- 真偽確認: gapMin/gapMax ・ score ・ lo/hi を 0..5 / 0..13 で全網羅し、
-- soundInterval と takeAll の仮説を満たす入力に反例がないことを #eval で確認済み。

/-- Every score produced by a covered page lies in the certified interval. -/
private theorem inside_scores {f : Kernel} (hf : soundInterval f) {t p a b} (hc : covers p a b) :
    ∀ v ∈ scores f t p, inside (f.bounds t a b) v := by
  intro v hv
  obtain ⟨r, hr, hm⟩ : ∃ r ∈ p, r.map (f.score t) = some v := by
    simpa [scores] using hv
  obtain ⟨x, y, hr2, hscore⟩ : ∃ x y, some (x, y) ∈ p ∧ f.score t (x, y) = v := by
    cases r with
    | none => cases hm
    | some xy =>
      cases xy with
      | mk x y => exact ⟨x, y, hr, by simpa using hm⟩
  obtain ⟨hax, hby⟩ := hc (some (x, y)) hr2 x y rfl
  rw [← hscore]
  exact hf t a b x y hax hby

/-- The squared gap of two points of the rectangle is bracketed by the
    squared extremal gaps of the rectangle. -/
private theorem sound_gap_interval : soundInterval kernel := by
  intro t a b x y hx hy
  simp only [inside] at hx hy ⊢
  have hmin : gapMin a b ≤ gap x y := by
    unfold gapMin gap
    exact (Nat.max_le).2 ⟨by omega, by omega⟩
  have hmax : gap x y ≤ gapMax a b := by
    unfold gapMax gap
    by_cases hxy : x ≤ y
    · have h1 : y - x ≤ b.hi - a.lo := by omega
      have h2 : x - y = 0 := by omega
      rw [h2, Nat.zero_add]
      exact Nat.le_trans h1 (Nat.le_max_right _ _)
    · have hyx : y ≤ x := by omega
      have h1 : x - y ≤ a.hi - b.lo := by omega
      have h2 : y - x = 0 := by omega
      rw [h2, Nat.add_zero]
      exact Nat.le_trans h1 (Nat.le_max_left _ _)
  have hsqmin : (gapMin a b) ^ 2 ≤ (gap x y) ^ 2 := by
    rw [Nat.pow_two, Nat.pow_two]
    exact Nat.mul_le_mul hmin hmin
  have hsqmax : (gap x y) ^ 2 ≤ (gapMax a b) ^ 2 := by
    rw [Nat.pow_two, Nat.pow_two]
    exact Nat.mul_le_mul hmax hmax
  change (gapMin a b) ^ 2 + t ≤ (gap x y) ^ 2 + t ∧
    (gap x y) ^ 2 + t ≤ (gapMax a b) ^ 2 + t
  omega

theorem sound_TakeAll : TakeAll.Goal := by
  intro t p a b lo hi hcov hlo hhi
  rw [selected]
  refine List.filter_eq_self.mpr (fun v hv => ?_)
  obtain ⟨hl, hu⟩ := inside_scores sound_gap_interval hcov v hv
  rw [Bool.and_eq_true]
  exact ⟨by simpa using Nat.le_trans hlo hl, by simpa using Nat.lt_of_le_of_lt hu hhi⟩

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_QuadraticGap_TakeAll
