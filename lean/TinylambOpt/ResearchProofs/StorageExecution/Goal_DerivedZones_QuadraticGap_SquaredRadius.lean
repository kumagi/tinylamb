import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_QuadraticGap_SquaredRadius

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.QuadraticGap

-- Exhaustive check on a small grid: no counterexample to the radius inverse.
private def counterexamples : List (Nat × Nat × Nat × Nat) :=
  (List.range 8).flatMap fun t =>
  (List.range 8).flatMap fun r =>
  (List.range 8).flatMap fun x =>
  (List.range 8).filterMap fun y =>
    if decide (¬ (kernel.score t (x, y) ≤ r * r + t ↔ x ≤ y + r ∧ y ≤ x + r)) then
      some (t, r, x, y)
    else none

#eval counterexamples

/-- After cancelling the common shift `t`, squaring is strictly monotone on
    naturals, and the gap bound is exactly the two-sided radius condition. -/
theorem sound_SquaredRadius : QuadraticGap.SquaredRadius.Goal := by
  intro t r x y
  simp only [kernel, gap]
  rw [Nat.add_le_add_iff_right]
  rw [Nat.pow_two (x - y + (y - x))]
  rw [Nat.mul_self_le_mul_self_iff]
  omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_QuadraticGap_SquaredRadius
