import TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds.Goal_Q15DuckDB
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

theorem sound : TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds.Q15DuckDB.Goal := by
  intro rows
  induction rows with
  | nil => simp
  | cons r rows ih =>
      rcases r with ⟨address, price⟩
      by_cases ha : address = true
      · subst address
        simp at ih ⊢
        exact ih
      · have haf : address = false := by cases address <;> simp_all
        subst address
        by_cases hp : price > 500
        · have hp' : decide (price > 500) = true := by simp [hp]
          simp [hp'] at ih ⊢
          exact (List.Perm.cons (false, price) ih).trans List.perm_middle.symm
        · have hp' : decide (price > 500) = false := by simp [hp]
          simp [hp'] at ih ⊢
          exact ih
