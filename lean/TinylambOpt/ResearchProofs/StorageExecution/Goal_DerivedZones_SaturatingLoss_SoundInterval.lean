import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

/-!
Scan pruning can exploit derived bounds only when every matching row remains covered by
the block summary.

Proved

- `SaturatingLoss.SoundInterval` — Bound a computed group or predicate key from
  independent column min/max.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_SaturatingLoss_SoundInterval

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.SaturatingLoss

-- Exhaustive check on a small grid: no hypothesis-satisfying counterexample.
private def smallBounds : List Bounds :=
  (List.range 6).flatMap fun lo =>
    ((List.range 6).filter (fun hi => decide (lo ≤ hi))).map fun hi => ⟨lo, hi⟩

private def counterexamples : List (Nat × Bounds × Bounds × Nat × Nat) :=
  (List.range 6).flatMap fun t =>
  smallBounds.flatMap fun a =>
  smallBounds.flatMap fun b =>
  (List.range 6).flatMap fun x =>
  (List.range 6).filterMap fun y =>
    if decide (a.lo ≤ x ∧ x ≤ a.hi ∧ b.lo ≤ y ∧ y ≤ b.hi ∧
        ¬ ((kernel.bounds t a b).lo ≤ kernel.score t (x, y) ∧
           kernel.score t (x, y) ≤ (kernel.bounds t a b).hi)) then
      some (t, a, b, x, y)
    else none

#eval counterexamples

theorem sound_SoundInterval : SaturatingLoss.SoundInterval.Goal := by
  intro t a b x y hax hby
  rcases a with ⟨alo, ahi⟩
  rcases b with ⟨blo, bhi⟩
  simp [inside, kernel] at hax hby ⊢
  omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_SaturatingLoss_SoundInterval
