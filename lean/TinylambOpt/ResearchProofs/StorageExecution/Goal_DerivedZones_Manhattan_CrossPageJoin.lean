import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_Manhattan_CrossPageJoin

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.Manhattan

/-! Truth check on a small grid. For bounds with `lo ≤ hi ≤ 3` and keys in
`0..3`, enumerate two covered singleton pages; whenever the footer intervals
are separated, the two scores must differ. Any larger page is a union of such
singletons, so a counterexample here would refute the goal. -/
private def smallBounds : List Bounds :=
  (List.range 4).flatMap fun lo =>
    ((List.range 4).filter (fun hi => decide (lo ≤ hi))).map fun hi => ⟨lo, hi⟩

private def points (r : Bounds) : List Nat :=
  (List.range 4).filter (fun x => decide (r.lo ≤ x ∧ x ≤ r.hi))

private def cpjCounterexamples : List (Bounds × Bounds × Bounds × Bounds × Nat × Nat × Nat × Nat × Nat × Nat) :=
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

/-- Each Manhattan column distance `gap x t` lies between the corresponding
`gapMin` and `gapMax`, and the two column certificates add. -/
private theorem soundManhattan : soundInterval kernel := by
  intro t a b x y hax hby
  obtain ⟨haxlo, haxhi⟩ := hax
  obtain ⟨hblo, hbhi⟩ := hby
  simp only [inside, kernel, gapMin, gapMax, gap]
  constructor <;> omega

private theorem score_mem {t : Nat} {p : List (Option Row)} {v : Nat}
    (hv : v ∈ scores kernel t p) :
    ∃ r ∈ p, ∃ x y, r = some (x, y) ∧ v = kernel.score t (x, y) := by
  rcases List.mem_filterMap.mp hv with ⟨r, hr, hmap⟩
  rcases Option.map_eq_some_iff.mp hmap with ⟨xy, hrxy, hscore⟩
  rcases xy with ⟨x, y⟩
  exact ⟨r, hr, x, y, hrxy, hscore.symm⟩

private theorem score_bounds {t : Nat} {p : List (Option Row)} {a b : Bounds}
    (hc : covers p a b) {v : Nat} (hv : v ∈ scores kernel t p) :
    inside (kernel.bounds t a b) v := by
  rcases score_mem hv with ⟨r, hr, x, y, hrxy, rfl⟩
  have hcov := hc r hr x y hrxy
  exact soundManhattan t a b x y hcov.1 hcov.2

theorem sound_CrossPageJoin : Manhattan.CrossPageJoin.Goal := by
  intro t u p q a b c d hcp hcq hsep x hx hxq
  have hb1 := score_bounds (t := t) (p := p) (a := a) (b := b) hcp hx
  have hb2 := score_bounds (t := u) (p := q) (a := c) (b := d) hcq hxq
  simp only [inside] at hb1 hb2
  omega

#guard cpjCounterexamples.length = 0

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_Manhattan_CrossPageJoin
