import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

/-!
Scan pruning can exploit derived bounds only when every matching row remains covered by
the block summary.

Proved

- `QuadraticGap.CrossPageJoin` — Disjoint derived score intervals imply an empty
  equijoin between pages.
- `QuadraticGap.SoundInterval` — Bound a computed group or predicate key from
  independent column min/max.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_QuadraticGap_CrossPageJoin

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.QuadraticGap

/-- The derived squared-gap interval is sound: every score of a row inside the
    two column rectangles lies between the certified lower and upper bounds. -/
private theorem sound : QuadraticGap.SoundInterval.Goal := by
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

theorem sound_CrossPageJoin : QuadraticGap.CrossPageJoin.Goal := by
  intro t u p q a b c d hcovp hcovq hgap
  intro x hxmem
  simp only [scores] at hxmem
  rw [List.mem_filterMap] at hxmem
  rcases hxmem with ⟨r, hrp, hxeq⟩
  cases r with
  | none => simp at hxeq
  | some row =>
    rcases row with ⟨x₁, y₁⟩
    simp at hxeq
    have hcovrow := hcovp (some (x₁, y₁)) hrp x₁ y₁ rfl
    have hxle : x ≤ (kernel.bounds t a b).hi := by
      rw [← hxeq]
      exact (sound t a b x₁ y₁ hcovrow.1 hcovrow.2).2
    intro hymem
    simp only [scores] at hymem
    rw [List.mem_filterMap] at hymem
    rcases hymem with ⟨s, hsq, hyeq⟩
    cases s with
    | none => simp at hyeq
    | some row2 =>
      rcases row2 with ⟨x₂, y₂⟩
      simp at hyeq
      have hcovrow2 := hcovq (some (x₂, y₂)) hsq x₂ y₂ rfl
      have hyle : (kernel.bounds u c d).lo ≤ kernel.score u (x₂, y₂) :=
        (sound u c d x₂ y₂ hcovrow2.1 hcovrow2.2).1
      rw [hyeq] at hyle
      omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_QuadraticGap_CrossPageJoin
