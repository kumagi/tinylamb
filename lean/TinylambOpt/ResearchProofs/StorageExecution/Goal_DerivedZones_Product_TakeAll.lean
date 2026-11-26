import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

/-!
Scan pruning can exploit derived bounds only when every matching row remains covered by
the block summary.

Proved

- `Product.TakeAll` — Eliminate the scalar predicate once the entire score interval
  fits.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_Product_TakeAll

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.Product

theorem sound_TakeAll : TakeAll.Goal := by
  intro t p a b lo hi hcov hlo hhi
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
        have hvxy : v = x * y := by simpa [kernel] using hr.symm
        subst v
        have hlo' : lo ≤ x * y := by
          exact Nat.le_trans hlo (Nat.mul_le_mul haxl hbyl)
        have hhi' : x * y < hi := by
          exact Nat.lt_of_le_of_lt (Nat.mul_le_mul haxh hbyh) hhi
        simp [hlo', hhi']
  unfold selected
  exact List.filter_eq_self.mpr hmem

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_Product_TakeAll
