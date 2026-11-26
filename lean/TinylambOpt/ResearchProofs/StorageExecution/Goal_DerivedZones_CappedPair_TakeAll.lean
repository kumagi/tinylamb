import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_CappedPair_TakeAll

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.CappedPair

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
        have hvxy : v = min t (min x y) := by simpa [kernel] using hr.symm
        subst v
        simp [kernel] at hlo hhi
        have hlo' : lo ≤ min t (min x y) := by
          exact Nat.le_trans hlo (by omega)
        have hhi' : min t (min x y) < hi := by
          exact Nat.lt_of_le_of_lt (by omega) hhi
        simp [hlo', hhi']
  unfold selected
  exact List.filter_eq_self.mpr hmem

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_CappedPair_TakeAll
