import TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds.Goal_Q83DuckDB
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

theorem sound : TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds.Q83DuckDB.Goal := by
  intro weeks returns
  simp [List.mem_eraseDups]

end TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds.Goal_Q83DuckDB

#print axioms TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds.Goal_Q83DuckDB.sound
