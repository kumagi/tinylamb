import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

/-!
Scan pruning can exploit derived bounds only when every matching row remains covered by
the block summary.

Proved

- `Manhattan.InteriorMinimum` — `∀ t a b, inside a t → inside b t → (kernel.bounds t a
  b).lo=0`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_Manhattan_InteriorMinimum

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.Manhattan

-- Exhaustive small-grid check: every bounds pair with lo <= hi <= 5 and every
-- t in 0..5; no instance with t inside both rectangles has a nonzero lower
-- certificate, so the goal is true rather than refutable.
private def smallBounds : List Bounds :=
  (List.range 6).flatMap fun lo =>
    ((List.range 6).filter (fun hi => decide (lo ≤ hi))).map fun hi => ⟨lo, hi⟩

private def counterexamples : List (Bounds × Bounds × Nat) :=
  smallBounds.flatMap fun a =>
  smallBounds.flatMap fun b =>
  (List.range 6).filterMap fun t =>
    if decide (a.lo ≤ t ∧ t ≤ a.hi ∧ b.lo ≤ t ∧ t ≤ b.hi ∧
        (kernel.bounds t a b).lo ≠ 0) then
      some (a, b, t)
    else none

#eval counterexamples.length

theorem sound_InteriorMinimum : Manhattan.InteriorMinimum.Goal := by
  rintro t ⟨alo, ahi⟩ ⟨blo, bhi⟩ hala hblb
  change alo ≤ t ∧ t ≤ ahi at hala
  change blo ≤ t ∧ t ≤ bhi at hblb
  rcases hala with ⟨halo, hahi⟩
  rcases hblb with ⟨hblo, hbhi⟩
  change max (alo - t) (t - ahi) + max (blo - t) (t - bhi) = 0
  have ha1 : alo - t = 0 := by omega
  have ha2 : t - ahi = 0 := by omega
  have hb1 : blo - t = 0 := by omega
  have hb2 : t - bhi = 0 := by omega
  simp [ha1, ha2, hb1, hb2]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_Manhattan_InteriorMinimum
