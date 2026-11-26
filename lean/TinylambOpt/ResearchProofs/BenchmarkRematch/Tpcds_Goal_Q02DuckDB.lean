import TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds.Goal_Q02DuckDB
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

theorem sound : TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds.Q02DuckDB.Goal := by
  intro a b
  cases ha : a.filterMap id with
  | nil =>
      cases hb : b.filterMap id with
      | nil => simp [nsum, ha, hb, List.filterMap_append]
      | cons y ys => simp [nsum, ha, hb, List.filterMap_append]
  | cons x xs =>
      cases hb : b.filterMap id with
      | nil => simp [nsum, ha, hb, List.filterMap_append]
      | cons y ys => simp [nsum, ha, hb, List.filterMap_append, List.sum_append]; omega
