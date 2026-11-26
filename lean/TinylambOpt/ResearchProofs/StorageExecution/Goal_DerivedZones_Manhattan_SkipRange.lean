import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

/-!
Scan pruning can exploit derived bounds only when every matching row remains covered by
the block summary.

Proved

- `Manhattan.SkipRange` — Drop a whole page for a range test, preserving empty and NULL
  pages.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_Manhattan_SkipRange

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.Manhattan

/-! Truth check on a small grid: every pair of valid bounds with lo <= hi <= 5
and every t, x, y in 0..5 satisfying the hypotheses must land in the
certificate. -/
private def smallBounds : List Bounds :=
  (List.range 6).flatMap fun lo =>
    ((List.range 6).filter (fun hi => decide (lo ≤ hi))).map fun hi => ⟨lo, hi⟩

private def soundCounterexamples : List (Bounds × Bounds × Nat × Nat × Nat) :=
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

#eval soundCounterexamples.length

/-- The two independent column intervals bound the Manhattan score: each
`gap x t` is between `gapMin a ⟨t,t⟩` and `gapMax a ⟨t,t⟩`, and the bounds
add, so the sum of the two column certificates contains the score. -/
private theorem soundManhattan : soundInterval kernel := by
  intro t a b x y hax hby
  obtain ⟨haxlo, haxhi⟩ := hax
  obtain ⟨hblo, hbhi⟩ := hby
  simp only [inside, kernel, gapMin, gapMax, gap]
  constructor <;> omega

/-- A certified scalar interval justifies dropping a whole page for a range
test: any score selected by `[lo, hi)` would force the footer to allow it. -/
private theorem skipRange_of_sound {f : Kernel} (hs : soundInterval f) : skipRange f := by
  intro t p a b lo hi hc hn
  unfold selected
  apply List.filter_eq_nil_iff.mpr
  intro v hv hpass
  have hpred : (lo ≤ v && v < hi) = true := hpass
  simp only [Bool.and_eq_true, decide_eq_true_eq] at hpred
  rcases hpred with ⟨hlo, hhi'⟩
  rcases List.mem_filterMap.mp hv with ⟨r, hr, hmap⟩
  rcases Option.map_eq_some_iff.mp hmap with ⟨xy, hrxy, hscore⟩
  rcases xy with ⟨x, y⟩
  have hcov := hc r hr x y hrxy
  have hb : inside (f.bounds t a b) (f.score t (x, y)) :=
    hs t a b x y hcov.1 hcov.2
  simp only [inside] at hb
  have hl : lo ≤ (f.bounds t a b).hi :=
    Nat.le_trans hlo (by simpa [hscore] using hb.2)
  have hr' : (f.bounds t a b).lo < hi :=
    Nat.lt_of_le_of_lt (by simpa [hscore] using hb.1) hhi'
  have hboth : lo < hi := Nat.lt_of_le_of_lt hlo hhi'
  have hallows : allows (f.bounds t a b) lo hi = true := by
    simp [allows, hl, hr', hboth]
  rw [hallows] at hn
  cases hn

theorem sound_SkipRange : Manhattan.SkipRange.Goal :=
  skipRange_of_sound soundManhattan

#guard allows (kernel.bounds 5 ⟨3, 7⟩ ⟨4, 8⟩) 6 7 == false
#guard selected kernel 5 [some (3, 7), none, some (7, 3)] 6 7 == []

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_Manhattan_SkipRange
