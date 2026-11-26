import TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds_Q02DuckDB

open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

theorem sound : Q02DuckDB.Goal := by
  intro a b
  simp only [nsum]
  rw [List.filterMap_append]
  by_cases hA : List.filterMap id a = [] <;> by_cases hB : List.filterMap id b = [] <;>
    simp [hA, hB]

end TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds_Q02DuckDB
