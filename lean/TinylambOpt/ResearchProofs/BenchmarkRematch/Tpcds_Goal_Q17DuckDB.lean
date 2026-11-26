import TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds.Goal_Q17DuckDB
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

theorem sound : TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds.Q17DuckDB.Goal := by
  intro xs n
  induction xs with
  | nil => simp [total]
  | cons x xs ih =>
      simp [total] at ih ⊢
      rw [ih]
      rw [Int.mul_add]
