import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_CappedPair_SumCeiling

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.CappedPair

private theorem sum_bounds (lo hi : Nat) (l : List Nat)
    (h : ∀ v ∈ l, lo ≤ v ∧ v ≤ hi) :
    l.length * lo ≤ l.sum ∧ l.sum ≤ l.length * hi := by
  induction l with
  | nil => simp
  | cons v vs ih =>
      have hv := h v (by simp)
      have hvs : ∀ w ∈ vs, lo ≤ w ∧ w ≤ hi := by
        intro w hw
        exact h w (by simp [hw])
      have ih' := ih hvs
      simp only [List.length_cons, List.sum_cons, Nat.add_mul, Nat.one_mul]
      constructor <;> omega

private theorem score_bounds (tv : Nat) (a b : Bounds) (x y : Nat)
    (hx : inside a x) (hy : inside b y) :
    min tv (min a.lo b.lo) ≤ min tv (min x y) ∧
    min tv (min x y) ≤ min tv (min a.hi b.hi) := by
  simp [inside] at hx hy
  omega

theorem sound_SumCeiling : CappedPair.SumCeiling.Goal := by
  intro tv p a b hcovers
  dsimp [sumCeiling]
  apply sum_bounds (min tv (min a.lo b.lo)) (min tv (min a.hi b.hi))
  intro v hv
  rw [scores] at hv
  rcases List.mem_filterMap.mp hv with ⟨r, hrp, hrv⟩
  cases r with
  | none => simp at hrv
  | some r =>
      rcases r with ⟨x, y⟩
      simp [kernel] at hrv
      subst v
      have hcov := hcovers (some (x, y)) hrp x y rfl
      exact score_bounds tv a b x y hcov.1 hcov.2

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_CappedPair_SumCeiling
