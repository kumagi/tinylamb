import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

/-!
Scan pruning can exploit derived bounds only when every matching row remains covered by
the block summary.

Proved

- `FloorRatio.CrossPageJoin` — Disjoint derived score intervals imply an empty equijoin
  between pages.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_FloorRatio_CrossPageJoin

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.FloorRatio

/-! Truth check on a small grid. A score produced by any covered page comes
from a single row inside the two zones, so singleton pages suffice to find a
counterexample. -/
private def smallBounds : List Bounds :=
  (List.range 4).flatMap fun lo =>
    ((List.range 4).filter (fun hi => decide (lo ≤ hi))).map fun hi => ⟨lo, hi⟩

private def points (r : Bounds) : List Nat :=
  (List.range 4).filter (fun x => decide (r.lo ≤ x ∧ x ≤ r.hi))

private def cpjCounterexamples :
    List (Bounds × Bounds × Bounds × Bounds × Nat × Nat × Nat × Nat × Nat × Nat) :=
  smallBounds.flatMap fun a =>
  smallBounds.flatMap fun b =>
  smallBounds.flatMap fun c =>
  smallBounds.flatMap fun d =>
  (List.range 4).flatMap fun t =>
  (List.range 4).flatMap fun u =>
  if !decide ((kernel.bounds t a b).hi < (kernel.bounds u c d).lo) then [] else
  (points a).flatMap fun x =>
  (points b).flatMap fun y =>
  (points c).flatMap fun x' =>
  (points d).filterMap fun y' =>
    if decide (kernel.score t (x, y) = kernel.score u (x', y')) then
      some (a, b, c, d, t, u, x, y, x', y')
    else none

#eval cpjCounterexamples.length

-- Reused soundness of the derived interval from independent column bounds.
private theorem soundIntervalFloorRatio : soundInterval kernel := by
  intro t a b x y hx hy
  obtain ⟨ha1, ha2⟩ := hx
  obtain ⟨hb1, hb2⟩ := hy
  constructor
  · exact Nat.div_le_div (Nat.mul_le_mul ha1 (Nat.le_refl t)) (by omega) (by omega)
  · exact Nat.div_le_div (Nat.mul_le_mul ha2 (Nat.le_refl t)) (by omega) (by omega)

private theorem inside_scores {t p a b} (hc : covers p a b) :
    ∀ v ∈ scores kernel t p, inside (kernel.bounds t a b) v := by
  intro v hv
  obtain ⟨r, hr, hm⟩ : ∃ r ∈ p, r.map (kernel.score t) = some v := by
    simpa [scores] using hv
  obtain ⟨x, y, hr2, hscore⟩ : ∃ x y, some (x, y) ∈ p ∧ kernel.score t (x, y) = v := by
    cases r with
    | none => cases hm
    | some xy =>
      cases xy with
      | mk x y => exact ⟨x, y, hr, by simpa using hm⟩
  obtain ⟨hax, hby⟩ := hc (some (x, y)) hr2 x y rfl
  rw [← hscore]
  exact soundIntervalFloorRatio t a b x y hax hby

theorem sound_CrossPageJoin : FloorRatio.CrossPageJoin.Goal := by
  intro t u p q a b c d hcp hcq hsep x hx hxq
  obtain ⟨_, hup⟩ := inside_scores hcp x hx
  obtain ⟨hlo, _⟩ := inside_scores hcq x hxq
  exact Nat.lt_irrefl x (Nat.lt_of_lt_of_le (Nat.lt_of_le_of_lt hup hsep) hlo)

#guard cpjCounterexamples.length = 0

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_FloorRatio_CrossPageJoin
