import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_BucketSum_TakeAll

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.BucketSum

theorem sound_TakeAll : BucketSum.TakeAll.Goal := by
  intro t p a b lo hi hcov hlo hhi
  unfold selected scores
  apply List.filter_eq_self.2
  intro v hv
  rw [List.mem_filterMap] at hv
  rcases hv with ⟨r, hrp, hrv⟩
  cases r with
  | none => simp at hrv
  | some xy =>
      rcases xy with ⟨x, y⟩
      simp at hrv
      subst v
      have hinside := hcov (some (x, y)) hrp x y rfl
      rcases hinside with ⟨hxa, hyb⟩
      have hlo_bounds : lo ≤ (a.lo + b.lo) / (t + 1) := by
        simpa [kernel] using hlo
      have hhi_bounds : (a.hi + b.hi) / (t + 1) < hi := by
        simpa [kernel] using hhi
      have hdiv_lo : (a.lo + b.lo) / (t + 1) ≤ (x + y) / (t + 1) :=
        Nat.div_le_div_right (Nat.add_le_add hxa.1 hyb.1)
      have hdiv_hi : (x + y) / (t + 1) ≤ (a.hi + b.hi) / (t + 1) :=
        Nat.div_le_div_right (Nat.add_le_add hxa.2 hyb.2)
      have hvlo : lo ≤ (x + y) / (t + 1) := Nat.le_trans hlo_bounds hdiv_lo
      have hvhi : (x + y) / (t + 1) < hi := Nat.lt_of_le_of_lt hdiv_hi hhi_bounds
      simp [kernel, hvlo, hvhi]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_BucketSum_TakeAll
