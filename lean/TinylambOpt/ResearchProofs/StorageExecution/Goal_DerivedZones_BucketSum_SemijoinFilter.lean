import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

/-!
Scan pruning can exploit derived bounds only when every matching row remains covered by
the block summary.

Proved

- `BucketSum.SemijoinFilter` — Apply the footer to the build-key set before probing;
  retain left bag order and duplicates.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_BucketSum_SemijoinFilter

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.BucketSum

/-- Small-grid truth check: division by a positive constant is monotone, so a
    covered row's bucket score always lies in the derived rectangle and no key
    set can distinguish the two filtered lists. -/
private def sublists : List Nat → List (List Nat)
  | [] => [[]]
  | x :: xs => (sublists xs).flatMap (fun ys => [ys, x :: ys])

private def smallBounds : List Bounds :=
  (List.range 4).flatMap fun lo =>
    ((List.range 4).filter fun hi => decide (lo ≤ hi)).map fun hi => ⟨lo, hi⟩

private def counterexamples : List (Nat × Bounds × Bounds × List Nat × Nat × Nat) :=
  (List.range 4).flatMap fun t =>
  smallBounds.flatMap fun a =>
  smallBounds.flatMap fun b =>
  (sublists [0, 1, 2, 3, 4, 5, 6]).flatMap fun ks =>
  (List.range 4).flatMap fun x =>
  (List.range 4).filterMap fun y =>
    if a.lo ≤ x ∧ x ≤ a.hi ∧ b.lo ≤ y ∧ y ≤ b.hi then
      let p : List (Option Row) := [some (x, y), none, some (x, y)]
      let r := kernel.bounds t a b
      if (scores kernel t p).filter ks.contains ≠
          (scores kernel t p).filter ((ks.filter (fun k => r.lo ≤ k && k ≤ r.hi)).contains) then
        some (t, a, b, ks, x, y)
      else none
    else none

#eval counterexamples.length

/-- Every score of a covered page lies in the BucketSum interval: addition is
    monotone in both arguments and Nat division is monotone in the numerator. -/
private theorem score_in_bounds (t : Nat) (p : List (Option Row)) (a b : Bounds)
    (hcov : covers p a b) (v : Nat) (hv : v ∈ scores kernel t p) :
    ((kernel.bounds t a b).lo ≤ v && v ≤ (kernel.bounds t a b).hi) = true := by
  unfold scores at hv
  rw [List.mem_filterMap] at hv
  rcases hv with ⟨r, hrp, hr⟩
  cases r with
  | none => simp at hr
  | some xy =>
      rcases xy with ⟨x, y⟩
      have hb := hcov (some (x, y)) hrp x y rfl
      simp only [inside] at hb
      rcases hb with ⟨⟨haxl, haxh⟩, ⟨hbyl, hbyh⟩⟩
      simp only [Option.map_some, Option.some.injEq] at hr
      subst v
      have hlow : a.lo + b.lo ≤ x + y := Nat.add_le_add haxl hbyl
      have hhigh : x + y ≤ a.hi + b.hi := Nat.add_le_add haxh hbyh
      dsimp [kernel]
      simp [Nat.div_le_div_right hlow, Nat.div_le_div_right hhigh]

/-- Restricting the build keys to the page's score interval is a no-op on
    scores actually produced by the page, so the semijoin filter is unchanged. -/
theorem sound_SemijoinFilter : SemijoinFilter.Goal := by
  intro t p a b ks hcov
  change (scores kernel t p).filter ks.contains =
    (scores kernel t p).filter
      ((ks.filter (fun k => (kernel.bounds t a b).lo ≤ k && k ≤ (kernel.bounds t a b).hi)).contains)
  apply List.filter_congr
  intro v hv
  have hvR := score_in_bounds t p a b hcov v hv
  simp only [List.contains]
  simp [hvR]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_BucketSum_SemijoinFilter
