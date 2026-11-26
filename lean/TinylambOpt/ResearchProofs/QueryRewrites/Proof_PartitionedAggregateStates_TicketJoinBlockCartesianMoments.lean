import TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates

namespace TinylambOpt.ResearchProofs.QueryRewrites.PartitionedAggregateStates.Goal_TicketJoinBlockCartesianMoments

open TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates


/-!
Factoring a three-way cartesian ticket join into per-branch subtotals preserves
each physical quantity column. Expanding one branch repeats every other branch
subtotal exactly the number of tuples in the remaining branches.
-/

set_option maxHeartbeats 0
set_option linter.unusedSimpArgs false

open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates

/-- Summing over a flat-mapped list is the sum of the per-element sub-sums. -/
theorem sum_flatMap {α β} (xs : List α) (f : α → List β) (g : β → Int) :
    ((xs.flatMap f).map g).sum = (xs.map (fun x => ((f x).map g).sum)).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simpa [List.sum_append, List.sum_cons] using congrArg (fun t => ((f x).map g).sum + t) ih

/-- A constant payload contributes its value once per row. -/
theorem sum_const {α} (xs : List α) (c : Int) :
    (xs.map (fun _ => c)).sum = (xs.length : Int) * c := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simpa [List.sum_cons, Int.add_mul, Int.one_mul, Int.add_comm, Int.add_left_comm, Int.add_assoc]
        using congrArg (fun t => c + t) ih

/-- A common integer factor can be pulled out of a list sum. -/
theorem sum_mul_const {α} (xs : List α) (k : Int) (f : α → Int) :
    (xs.map (fun x => k * f x)).sum = k * (xs.map f).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih => simp [List.sum_cons, ih, Int.mul_add]

/-- A common right-hand factor can be pulled out of a list sum. -/
theorem sum_mul_const_right {α} (xs : List α) (f : α → Int) (k : Int) :
    (xs.map (fun x => f x * k)).sum = (xs.map f).sum * k := by
  induction xs with
  | nil => simp
  | cons x xs ih => simp [List.sum_cons, ih, Int.add_mul]

theorem sound : TicketJoinBlockCartesianMoments.Goal := by
  intro s r c
  apply Prod.ext
  · simp [total, sum_flatMap, sum_const, sum_mul_const, sum_mul_const_right, Function.comp_def, Int.mul_assoc, Int.mul_comm, Int.mul_left_comm]
  · apply Prod.ext
    · simp [total, sum_flatMap, sum_const, sum_mul_const, sum_mul_const_right, Function.comp_def, Int.mul_assoc, Int.mul_comm, Int.mul_left_comm]
    · simp [total, sum_flatMap, sum_const, sum_mul_const, sum_mul_const_right, Function.comp_def, Int.mul_assoc, Int.mul_comm, Int.mul_left_comm]
end TinylambOpt.ResearchProofs.QueryRewrites.PartitionedAggregateStates.Goal_TicketJoinBlockCartesianMoments
