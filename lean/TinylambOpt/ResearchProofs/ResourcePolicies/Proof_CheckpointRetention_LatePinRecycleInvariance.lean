import TinylambOpt.ResearchGoals.ResourcePolicies.CheckpointRetention

namespace TinylambOpt.ResearchProofs.ResourcePolicies.CheckpointRetention.Goal_LatePinRecycleInvariance

open TinylambOpt.ResearchGoals.ResourcePolicies.CheckpointRetention


/-! Adding a pin that starts not before the current floor cannot lower the
floor. The fold over pins is a commutative/associative minimum, so prepending
the pin merely inserts `min` into an already smaller accumulator. -/

open TinylambOpt.ResearchGoals.ResourcePolicies.CheckpointRetention

private theorem floorAt_min_start (ps : List Nat) (a b : Nat) :
    floorAt (min a b) ps = min a (floorAt b ps) := by
  simpa [floorAt] using
    (List.foldl_assoc (op := min) (l := ps) (a₁ := a) (a₂ := b))

private theorem floorAt_cons (cp x : Nat) (pins : List Nat) :
    floorAt cp (x :: pins) = min x (floorAt cp pins) := by
  cases pins with
  | nil => simp [floorAt, Nat.min_comm]
  | cons p ps =>
      change floorAt (min (min cp x) p) ps = min x (floorAt (min cp p) ps)
      rw [show min (min cp x) p = min x (min cp p) by omega]
      exact floorAt_min_start ps x (min cp p)

theorem sound :
    TinylambOpt.ResearchGoals.ResourcePolicies.CheckpointRetention.LatePinRecycleInvariance.Goal := by
  intro segs cp pins x hx
  rw [floorAt_cons]
  rw [Nat.min_eq_right hx]
end TinylambOpt.ResearchProofs.ResourcePolicies.CheckpointRetention.Goal_LatePinRecycleInvariance
