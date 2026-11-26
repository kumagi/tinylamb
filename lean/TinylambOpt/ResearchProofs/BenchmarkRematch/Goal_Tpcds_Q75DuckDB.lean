import TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds_Q75DuckDB
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

/-- Exact widened arithmetic: the running total of `q - v` splits into a
multiplicity term minus the refund sum. -/
private theorem sub_total (q : Int) : ∀ (xs : List Int),
    (xs.map (fun v => q - v)).sum = (xs.length : Int) * q - xs.sum := by
  intro xs
  induction xs with
  | nil => simp only [List.map_nil, List.sum_nil, List.length_nil]; omega
  | cons v ys ih =>
    rw [List.map_cons, List.sum_cons, ih, List.length_cons, List.sum_cons]
    rw [show ((ys.length + 1 : Nat) : Int) = (ys.length : Int) + 1 from by simp,
        Int.add_mul]
    omega

theorem sound : Q75DuckDB.Goal := by
  intro q rs
  cases rs with
  | nil =>
    simp only [List.isEmpty_nil, ite_true, total, List.map_nil, List.sum_nil, List.length_nil]
    rw [Int.max_eq_left (by omega)]
    omega
  | cons v ys =>
    simp only [List.isEmpty_cons, Bool.false_eq_true, ite_false, total, List.length_cons]
    rw [Int.max_eq_right (by omega), sub_total, List.length_cons]

#print axioms TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds_Q75DuckDB.sound
