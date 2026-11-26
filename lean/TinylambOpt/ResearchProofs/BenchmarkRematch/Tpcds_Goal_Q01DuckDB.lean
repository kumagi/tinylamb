import TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds.Goal_Q01DuckDB
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

theorem sound : TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds.Q01DuckDB.Goal := by
  intro returns tn store htn
  rw [List.filter_filter]
  apply List.filter_congr
  intro r hr
  by_cases h : r.1 == store
  · have heq : r.1 = store := beq_iff_eq.mp h
    rw [heq]
    simp [htn]
  · simp [h]
