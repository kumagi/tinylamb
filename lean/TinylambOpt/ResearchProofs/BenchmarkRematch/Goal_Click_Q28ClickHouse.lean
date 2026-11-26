import TinylambOpt.ResearchGoals.BenchmarkRematch.Click

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Click_Q28ClickHouse
open TinylambOpt.ResearchGoals.BenchmarkRematch.Click

private theorem sum_filter_ne_zero : ∀ (xs : List Nat), (xs.filter (· != 0)).sum = xs.sum := by
  intro xs
  induction xs with
  | nil => rfl
  | cons x t ih =>
    cases x with
    | zero => simp [ih]
    | succ n => simp [ih]

private theorem length_filter_ne_zero :
    ∀ (xs : List Nat), (xs.filter (· != 0)).length = xs.length - xs.count 0 := by
  intro xs
  have hpar := List.length_eq_countP_add_countP (fun x => x != 0) (l := xs)
  have hneg : List.countP (fun a => ¬(a != 0)) xs = xs.count 0 := by
    rw [List.count_eq_countP]
    refine List.countP_congr (fun a _ => ?_)
    simp
  have hpar := List.length_eq_countP_add_countP (fun x => x != 0) (l := xs)
  rw [hneg] at hpar
  rw [List.countP_eq_length_filter] at hpar
  omega

theorem sound : Q28ClickHouse.Goal := by
  intro lengths
  rw [sum_filter_ne_zero, length_filter_ne_zero]

end TinylambOpt.ResearchProofs.BenchmarkRematch.Click_Q28ClickHouse
