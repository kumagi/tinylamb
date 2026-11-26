import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

/-!
Scan pruning can exploit derived bounds only when every matching row remains covered by
the block summary.

Proved

- `Distance.TakeAll` — Eliminate the scalar predicate once the entire score interval
  fits.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_Distance_TakeAll

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.Distance

/-- The certified interval `[gapMin, gapMax]` contains every score `gap x y`
of a row inside the bounds, so a range covering the whole interval keeps every
score. -/
theorem sound_TakeAll : TakeAll.Goal := by
  intro t p a b lo hi hcov hlo hhi
  simp only [kernel] at hlo hhi
  have hmem : ∀ v ∈ scores kernel t p, (decide (lo ≤ v) && decide (v < hi)) = true := by
    intro v hv
    unfold scores at hv
    rw [List.mem_filterMap] at hv
    rcases hv with ⟨r, hrp, hr⟩
    cases r with
    | none => simp at hr
    | some xy =>
        rcases xy with ⟨x, y⟩
        have hb := hcov (some (x, y)) hrp x y rfl
        simp only [inside] at hb
        rcases hb with ⟨⟨haxl, haxh⟩, ⟨hbyl, hbyh⟩⟩
        simp only [Option.map_some] at hr
        have hvxy : v = gap x y := by simpa [kernel] using hr.symm
        subst v
        have hgaplo : gapMin a b ≤ gap x y := by
          dsimp [gapMin, gap]
          rw [Nat.max_le]
          constructor
          · have h1 : a.lo - b.hi ≤ x - b.hi := Nat.sub_le_sub_right haxl b.hi
            have h2 : x - b.hi ≤ x - y := Nat.sub_le_sub_left hbyh x
            omega
          · have h1 : b.lo - a.hi ≤ y - a.hi := Nat.sub_le_sub_right hbyl a.hi
            have h2 : y - a.hi ≤ y - x := Nat.sub_le_sub_left haxh y
            omega
        have hlo' : lo ≤ gap x y := Nat.le_trans hlo hgaplo
        have hgapmax : gap x y ≤ gapMax a b := by
          dsimp [gapMax, gap]
          by_cases hxy : x ≤ y
          · have h0 : x - y = 0 := Nat.sub_eq_zero_of_le hxy
            rw [h0, Nat.zero_add]
            have h1 : y - x ≤ b.hi - x := Nat.sub_le_sub_right hbyh x
            have h2 : b.hi - x ≤ b.hi - a.lo := Nat.sub_le_sub_left haxl b.hi
            exact Nat.le_trans (Nat.le_trans h1 h2) (Nat.le_max_right _ _)
          · have hyx : y ≤ x := Nat.le_of_lt (Nat.lt_of_not_ge hxy)
            have h0 : y - x = 0 := Nat.sub_eq_zero_of_le hyx
            rw [h0, Nat.add_zero]
            have h1 : x - y ≤ a.hi - y := Nat.sub_le_sub_right haxh y
            have h2 : a.hi - y ≤ a.hi - b.lo := Nat.sub_le_sub_left hbyl a.hi
            exact Nat.le_trans (Nat.le_trans h1 h2) (Nat.le_max_left _ _)
        have hhi' : gap x y < hi := Nat.lt_of_le_of_lt hgapmax hhi
        simp [hlo', hhi']
  unfold selected
  exact List.filter_eq_self.mpr hmem

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_Distance_TakeAll
