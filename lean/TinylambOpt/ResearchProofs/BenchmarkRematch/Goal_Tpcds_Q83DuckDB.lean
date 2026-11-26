import TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds_Q83DuckDB
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

/-- Probing the deduplicated week list answers the same membership question as
probing the raw list, so the return filter is unchanged. -/
theorem sound : Q83DuckDB.Goal := by
  intro weeks returns
  refine List.filter_congr (fun x _ => ?_)
  classical
  rw [Bool.eq_iff_iff]
  simp only [List.contains_eq_mem, decide_eq_true_eq, List.mem_eraseDups]

#print axioms TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds_Q83DuckDB.sound
