import TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds_Q87DuckDB
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

/-- Two successive NOT-IN anti-joins equal one anti-join against the union of
the exclusion tables. -/
theorem sound : Q87DuckDB.Goal := by
  intro s c w
  rw [List.filter_filter]
  refine List.filter_congr (fun x _ => ?_)
  rw [List.contains_append]
  cases hc : c.contains x <;> cases hw : w.contains x <;> rfl

#print axioms TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds_Q87DuckDB.sound
