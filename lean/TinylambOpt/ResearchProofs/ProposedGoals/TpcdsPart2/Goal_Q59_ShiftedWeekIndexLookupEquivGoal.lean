import TinylambOpt.ProposedGoals.BenchmarkRematch.TpcdsPart2

/-!
# Shifted-week indexed aggregate lookup (TPC-DS Q59)

Summing the product of each year-1 row with every matching shifted year-2 row equals
summing, per year-1 row, the product with the total of those year-2 rows. This is integer
distributivity over the matching rows, so the executor may index year 2 by
`(store, week - 52)` instead of materializing both full-year CTEs.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.ProposedGoals.TpcdsPart2.Goal_Q59_ShiftedWeekIndexLookupEquivGoal

open TinylambOpt.ProposedGoals.BenchmarkRematch.TpcdsPart2
open TinylambOpt.ResearchGoals.QueryRewrites

/-- A left factor distributes over the sum of the expanded right-hand matches. -/
theorem sum_map_mul_total {α : Type} (rows : List α) (left : α → Int)
    (right : List (Nat × Int)) (matchRight : α → Nat × Int → Bool) :
    (rows.flatMap (fun a => (right.filter (matchRight a)).map (fun b => left a * b.2))).sum =
      (rows.map (fun a => left a * total (right.filter (matchRight a)) Prod.snd)).sum := by
  induction rows with
  | nil => simp [total]
  | cons a rows ih =>
    simp only [List.flatMap_cons, List.map_cons, List.sum_cons, List.map_map, Function.comp_def,
      total] at ih ⊢
    rw [List.sum_append, ih]
    have hmul : ((right.filter (matchRight a)).map (fun b => left a * b.2)).sum =
        left a * ((right.filter (matchRight a)).map Prod.snd).sum := by
      induction right.filter (matchRight a) with
      | nil => simp
      | cons b bs h =>
        rw [List.map_cons, List.sum_cons, h]
        exact (Int.mul_add _ _ _).symm
    rw [hmul]

theorem sound : Q59_ShiftedWeekIndexLookupEquivGoal := by
  intro y1 y2 shift
  simpa [total] using sum_map_mul_total y1 Prod.snd y2 (fun a b => b.1 == a.1 + shift)

end TinylambOpt.ResearchProofs.ProposedGoals.TpcdsPart2.Goal_Q59_ShiftedWeekIndexLookupEquivGoal
