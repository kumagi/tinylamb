import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_SaturatingLoss_SemijoinFilter

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.SaturatingLoss

/-- Small-grid truth check: every covered row's score lies in the saturating
loss interval, so no key set can distinguish the two filtered lists. -/
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
          (scores kernel t p).filter ((ks.filter (fun k => r.lo ≤ k && k ≤ r.hi)).contains) then
        some (t, a, b, ks, x, y)
      else none
    else none

#eval counterexamples.length

/-- Nat subtraction is monotone in the minuend and antitone in the subtrahend,
so a score coming from a covered row stays inside the derived interval. -/
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
      rcases a with ⟨alo, ahi⟩
      rcases b with ⟨blo, bhi⟩
      have hlow : alo - (bhi + t) ≤ x - (y + t) :=
        Nat.le_trans (Nat.sub_le_sub_right haxl (bhi + t))
          (Nat.sub_le_sub_left (Nat.add_le_add_right hbyh t) x)
      have hhigh : x - (y + t) ≤ ahi - (blo + t) :=
        Nat.le_trans (Nat.sub_le_sub_right haxh (y + t))
          (Nat.sub_le_sub_left (Nat.add_le_add_right hbyl t) ahi)
      dsimp [kernel]
      simp [hlow, hhigh]

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

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_SaturatingLoss_SemijoinFilter
