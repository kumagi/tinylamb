import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_Manhattan_SoundInterval

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.Manhattan

-- Exhaustive small-grid check: every bounds pair with lo <= hi <= 5 and every
-- t, x, y in 0..5 satisfying the hypotheses must land inside the certificate.
private def smallBounds : List Bounds :=
  (List.range 6).flatMap fun lo =>
    ((List.range 6).filter (fun hi => decide (lo ≤ hi))).map fun hi => ⟨lo, hi⟩

private def counterexamples : List (Bounds × Bounds × Nat × Nat × Nat) :=
  smallBounds.flatMap fun a =>
  smallBounds.flatMap fun b =>
  (List.range 6).flatMap fun t =>
  (List.range 6).flatMap fun x =>
  (List.range 6).filterMap fun y =>
    if decide (a.lo ≤ x ∧ x ≤ a.hi ∧ b.lo ≤ y ∧ y ≤ b.hi ∧
        ¬ ((kernel.bounds t a b).lo ≤ kernel.score t (x, y) ∧
           kernel.score t (x, y) ≤ (kernel.bounds t a b).hi)) then
      some (a, b, t, x, y)
    else none

#eval counterexamples.length

theorem sound_SoundInterval : Manhattan.SoundInterval.Goal := by
  intro t a b x y hax hby
  obtain ⟨haxlo, haxhi⟩ := hax
  obtain ⟨hblo, hbhi⟩ := hby
  simp only [inside, kernel, gapMin, gapMax, gap]
  constructor <;> omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_Manhattan_SoundInterval
