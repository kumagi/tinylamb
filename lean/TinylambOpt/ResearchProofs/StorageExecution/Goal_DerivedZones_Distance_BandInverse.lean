import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_Distance_BandInverse

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.Distance

-- Exhaustive check on a small grid: no counterexample to the interval inverse.
private def counterexamples : List (Nat × Nat × Nat) :=
  (List.range 8).flatMap fun t =>
  (List.range 8).flatMap fun x =>
  (List.range 8).filterMap fun y =>
    if decide (¬ (kernel.score 0 (x, y) ≤ t ↔ x ≤ y + t ∧ y ≤ x + t)) then
      some (t, x, y)
    else none

#eval counterexamples

theorem sound_BandInverse : Distance.BandInverse.Goal := by
  intro t x y
  simp only [kernel, gap]
  omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_Distance_BandInverse
