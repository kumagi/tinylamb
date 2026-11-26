import TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds_Q59DuckDB
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

/-- Dropping zero-contribution rows never changes a sum. -/
private theorem sum_dropZeros {α : Type} (f : α → Nat) : ∀ (l : List α),
    (l.map f).sum = ((l.filter (fun x => f x != 0)).map f).sum := by
  intro l
  induction l with
  | nil => rfl
  | cons a as ih =>
    simp only [List.map_cons, List.sum_cons]
    by_cases hz : f a = 0
    · rw [List.filter_cons_of_neg (by simp [hz]), ih]
      simp [hz]
    · rw [List.filter_cons_of_pos (by simp [hz]), List.map_cons, List.sum_cons, ih]

theorem sound : Q59DuckDB.Goal := fun left right =>
  sum_dropZeros (fun r => right.count (r.1, r.2 + 52)) left

#print axioms TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds_Q59DuckDB.sound
