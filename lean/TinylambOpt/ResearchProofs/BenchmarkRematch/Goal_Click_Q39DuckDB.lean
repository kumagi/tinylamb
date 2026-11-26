import TinylambOpt.ResearchGoals.BenchmarkRematch.Click
import TinylambOpt.ResearchProofs.BenchmarkRematch.Topn

set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Click_Q39DuckDB
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Click
open TinylambOpt.ResearchProofs.BenchmarkRematch.Topn

theorem sound : Q39DuckDB.Goal := by
  intro shards
  rw [ranges_best _ 1000 10 1010 (by omega), ranges_best _ 1000 10 1010 (by omega), topn_flatMap]

end TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Click_Q39DuckDB

#print axioms TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Click_Q39DuckDB.sound
