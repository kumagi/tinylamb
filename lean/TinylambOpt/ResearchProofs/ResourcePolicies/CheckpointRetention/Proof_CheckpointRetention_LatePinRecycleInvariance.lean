import TinylambOpt.ResearchGoals.ResourcePolicies.CheckpointRetention
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace Nov26.Proof_CheckpointRetention_LatePinRecycleInvariance
open TinylambOpt.ResearchGoals.ResourcePolicies.CheckpointRetention

private theorem foldl_min_eq_of_le {x : Nat} : ∀ (l : List Nat) (a : Nat),
    l.foldl min a ≤ x → l.foldl min (min a x) = l.foldl min a := by
  intro l
  induction l with
  | nil =>
    intro a h
    simp at h
    simp [Nat.min_eq_left h]
  | cons y ys ih =>
    intro a h
    simp only [List.foldl_cons] at h ⊢
    have hmin : min (min a x) y = min (min a y) x := by omega
    rw [hmin]
    exact ih (min a y) h

theorem sound : LatePinRecycleInvariance.Goal := by
  intro segs cp pins x hfloor
  have heq := foldl_min_eq_of_le pins cp hfloor
  simp only [retained, floorAt, List.foldl_cons, heq]
  rfl
end Nov26.Proof_CheckpointRetention_LatePinRecycleInvariance
