import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
import TinylambOpt.ResearchProofs.StorageExecution.SE_DerivedZones_Distance

/-!
Scan pruning can exploit derived bounds only when every matching row remains covered by
the block summary.

Proved

- `Distance.SemijoinFilter` — Apply the footer to the build-key set before probing;
  retain left bag order and duplicates.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_Distance_SemijoinFilter

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.Distance

/-! Truth check on a small grid: every hypothesis-satisfying input must keep
the filtered build-key set unchanged once the keys are restricted to the
page's derived gap interval. -/
private def sublists : List Nat → List (List Nat)
  | [] => [[]]
  | x :: xs => (sublists xs).flatMap (fun ys => [ys, x :: ys])

private def smallBounds : List Bounds :=
  (List.range 5).flatMap fun lo =>
    ((List.range 5).filter fun hi => decide (lo ≤ hi)).map fun hi => ⟨lo, hi⟩

private def counterexamples : List (Nat × Bounds × Bounds × List Nat × Nat × Nat) :=
  (List.range 5).flatMap fun t =>
  smallBounds.flatMap fun a =>
  smallBounds.flatMap fun b =>
  (sublists [0, 1, 2, 3, 4]).flatMap fun ks =>
  (List.range 5).flatMap fun x =>
  (List.range 5).filterMap fun y =>
    if a.lo ≤ x ∧ x ≤ a.hi ∧ b.lo ≤ y ∧ y ≤ b.hi then
      let p : List (Option Row) := [some (x, y), none, some (x, y)]
      let r := kernel.bounds t a b
      if (scores kernel t p).filter ks.contains ≠
          (scores kernel t p).filter
            ((ks.filter (fun k => r.lo ≤ k && k ≤ r.hi)).contains) then
        some (t, a, b, ks, x, y)
      else none
    else none

#eval counterexamples.length

/-- Every score of a covered page lies in the derived gap interval. This is
exactly the (already proven) `Distance.soundInterval` instantiated on the
row's witnessing coordinates. -/
private theorem score_in_bounds (t : Nat) (p : List (Option Row)) (a b : Bounds)
    (hcov : covers p a b) (v : Nat) (hv : v ∈ scores kernel t p) :
    (kernel.bounds t a b).lo ≤ v ∧ v ≤ (kernel.bounds t a b).hi := by
  unfold scores at hv
  rw [List.mem_filterMap] at hv
  rcases hv with ⟨r, hrp, hr⟩
  cases r with
  | none => simp at hr
  | some xy =>
      rcases xy with ⟨x, y⟩
      have hb := hcov (some (x, y)) hrp x y rfl
      have hs :=
        TinylambOpt.ResearchProofs.StorageExecution.DerivedZones.Distance.sound_SoundInterval
          t a b x y hb.1 hb.2
      simp only [Option.map_some, Option.some.injEq] at hr
      subst v
      simpa [inside] using hs

/-- Restricting the build keys to the page's gap-score interval is a no-op on
scores actually produced by the page, so the semijoin filter is unchanged. -/
theorem sound_SemijoinFilter : SemijoinFilter.Goal := by
  intro t p a b ks hcov
  change (scores kernel t p).filter ks.contains =
    (scores kernel t p).filter
      ((ks.filter
        (fun k => (kernel.bounds t a b).lo ≤ k && k ≤ (kernel.bounds t a b).hi)).contains)
  apply List.filter_congr
  intro v hv
  have hvR := score_in_bounds t p a b hcov v hv
  simp only [List.contains]
  simp [hvR.1, hvR.2]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_Distance_SemijoinFilter
