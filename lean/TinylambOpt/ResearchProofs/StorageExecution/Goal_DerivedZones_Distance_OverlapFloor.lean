import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_Distance_OverlapFloor

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.Distance

-- Exhaustive check on a small grid: no counterexample to the overlap floor.
private def counterexamples : List (Nat × Nat × Nat × Nat) :=
  (List.range 5).flatMap fun al =>
  (List.range 5).flatMap fun ah =>
  (List.range 5).flatMap fun bl =>
  (List.range 5).filterMap fun bh =>
    let a : Bounds := ⟨al, ah⟩
    let b : Bounds := ⟨bl, bh⟩
    if decide (¬ ((kernel.bounds 0 a b).lo = 0 ↔ a.lo ≤ b.hi ∧ b.lo ≤ a.hi)) then
      some (al, ah, bl, bh)
    else none

#eval counterexamples

theorem sound_OverlapFloor : Distance.OverlapFloor.Goal := by
  intro a b
  simp only [kernel, gapMin]
  omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_Distance_OverlapFloor
