import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_SaturatingLoss_TakeAll

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.SaturatingLoss

theorem sound_TakeAll : TakeAll.Goal := by
  intro t p a b lo hi hcov hlo hhi
  simp [kernel] at hlo hhi
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
        have hvxy : v = x - (y + t) := by simpa [kernel] using hr.symm
        subst v
        have hlo' : lo ≤ x - (y + t) := by
          have h1 : a.lo - (b.hi + t) ≤ x - (b.hi + t) :=
            Nat.sub_le_sub_right haxl (b.hi + t)
          have h2 : x - (b.hi + t) ≤ x - (y + t) :=
            Nat.sub_le_sub_left (Nat.add_le_add_right hbyh t) x
          exact Nat.le_trans hlo (Nat.le_trans h1 h2)
        have hhi' : x - (y + t) < hi := by
          have h1 : x - (y + t) ≤ a.hi - (y + t) :=
            Nat.sub_le_sub_right haxh (y + t)
          have h2 : a.hi - (y + t) ≤ a.hi - (b.lo + t) :=
            Nat.sub_le_sub_left (Nat.add_le_add_right hbyl t) a.hi
          exact Nat.lt_of_le_of_lt (Nat.le_trans h1 h2) hhi
        simp [hlo', hhi']
  unfold selected
  exact List.filter_eq_self.mpr hmem

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_SaturatingLoss_TakeAll
