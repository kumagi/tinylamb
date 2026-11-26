import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

/-!
Scan pruning can exploit derived bounds only when every matching row remains covered by
the block summary.

Proved

- `FloorRatio.TakeAll` — Eliminate the scalar predicate once the entire score interval
  fits.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_FloorRatio_TakeAll

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.FloorRatio

/-- Division by a positive denominator is monotone in the numerator and
antitone in the denominator, so `(x*t)/(y+1)` lies between the two certified
corner values whenever `x ∈ [a.lo,a.hi]` and `y ∈ [b.lo,b.hi]`. Hence a range
covering the whole interval keeps every score. -/
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
        have hvxy : v = (x * t) / (y + 1) := by simpa [kernel] using hr.symm
        subst v
        have hlo' : lo ≤ (x * t) / (y + 1) := by
          have h1 : (a.lo * t) / (b.hi + 1) ≤ (x * t) / (b.hi + 1) :=
            Nat.div_le_div_right (Nat.mul_le_mul_right t haxl)
          have h2 : (x * t) / (b.hi + 1) ≤ (x * t) / (y + 1) :=
            Nat.div_le_div_left (Nat.add_le_add_right hbyh 1) (Nat.succ_pos y)
          exact Nat.le_trans hlo (Nat.le_trans h1 h2)
        have hhi' : (x * t) / (y + 1) < hi := by
          have h1 : (x * t) / (y + 1) ≤ (a.hi * t) / (y + 1) :=
            Nat.div_le_div_right (Nat.mul_le_mul_right t haxh)
          have h2 : (a.hi * t) / (y + 1) ≤ (a.hi * t) / (b.lo + 1) :=
            Nat.div_le_div_left (Nat.add_le_add_right hbyl 1) (Nat.succ_pos b.lo)
          exact Nat.lt_of_le_of_lt (Nat.le_trans h1 h2) hhi
        simp [hlo', hhi']
  unfold selected
  exact List.filter_eq_self.mpr hmem

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_FloorRatio_TakeAll
