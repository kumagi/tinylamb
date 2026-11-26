import TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance

/-!
Snapshot maintenance must preserve the rows visible to a SQL transaction across edits,
retention, and compaction.

Proved

- `BatchWatermarks.CoarseTime` — `∀ xs t step, 0 < step → score t xs = score (t/step)
  (rounded step xs) - ((xs.filter (fun r => r.1/step == t/step && t < r.1)).map
  Prod.snd).sum`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_BatchWatermarks_CoarseTime

open TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance
open TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance.BatchWatermarks

/-! Exhaustive small-input sanity check: enumerate sub-lists of a small log and
check that rounding each timestamp down to a bucket, then subtracting the
future rows in the query's boundary bucket, reconstructs the exact score. -/

def smallRows : List Row := [(0, 3), (1, -2), (2, 5), (3, -1), (5, 4)]

def sublists : List Row → List (List Row)
  | [] => [[]]
  | r :: rs => (sublists rs).flatMap (fun s => [s, r :: s])

def coarseRhs (xs : List Row) (t step : Nat) : Int :=
  score (t / step) (rounded step xs) -
    ((xs.filter (fun r => r.1 / step == t / step && t < r.1)).map Prod.snd).sum

#eval (sublists smallRows).all (fun xs =>
  (List.range 8).all (fun t =>
    ((List.range 6).map (· + 1)).all (fun step =>
      score t xs == coarseRhs xs t step)))

/-! Proof: rewrite every filtered sum as a sum of `if`-contributions, then
compare the three contributions row by row.  For one row `(a,w)` the identity is

  [a ≤ t]·w = [a/step ≤ t/step]·w - [a/step = t/step ∧ t < a]·w,

because `a ≤ t` implies `a/step ≤ t/step`, and when `t < a` but
`a/step ≤ t/step` the two quotients coincide (monotonicity both ways). -/

private theorem filter_sum_eq_ite_sum {α : Type} (p : α → Bool) (f : α → Int) (xs : List α) :
    ((xs.filter p).map f).sum = (xs.map (fun x => if p x then f x else 0)).sum := by
  induction xs with
  | nil => simp
  | cons a l ih =>
      by_cases h : p a <;> simp [h, ih]

private theorem pointwise (a t step : Nat) (w : Int) (_hstep : 0 < step) :
    (if a ≤ t then w else 0) =
      (if a / step ≤ t / step then w else 0) -
        (if a / step = t / step ∧ t < a then w else 0) := by
  by_cases hat : a ≤ t
  · have hdiv : a / step ≤ t / step := Nat.div_le_div_right hat
    have hnlt : ¬ t < a := by omega
    simp [hat, hdiv, hnlt]
  · have hlt : t < a := by omega
    have htdiv : t / step ≤ a / step := Nat.div_le_div_right (Nat.le_of_lt hlt)
    by_cases hdiv : a / step ≤ t / step
    · have heq : a / step = t / step := Nat.le_antisymm hdiv htdiv
      simp [hat, heq, hlt]
    · have hneq : a / step ≠ t / step := by omega
      simp [hat, hdiv, hneq]

private theorem sum_pointwise (xs : List Row) (t step : Nat) (hstep : 0 < step) :
    (xs.map (fun r => if r.1 ≤ t then r.2 else 0)).sum =
      (xs.map (fun r => if r.1 / step ≤ t / step then r.2 else 0)).sum -
        (xs.map (fun r => if r.1 / step = t / step ∧ t < r.1 then r.2 else 0)).sum := by
  induction xs with
  | nil => simp
  | cons r rs ih =>
      rcases r with ⟨a, w⟩
      simp only [List.map_cons, List.sum_cons]
      have hp := pointwise a t step w hstep
      omega

theorem sound_BatchWatermarks_CoarseTime :
    BatchWatermarks.CoarseTime.Goal := by
  intro xs t step hstep
  simp [score, visible, rounded, filter_sum_eq_ite_sum, Function.comp_def]
  exact sum_pointwise xs t step hstep

end TinylambOpt.ResearchProofs.Atlas.Goal_BatchWatermarks_CoarseTime
