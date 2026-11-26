import TinylambOpt.ResearchGoals.BenchmarkRematch.Tpch
import TinylambOpt.ResearchProofs.BenchmarkRematch.Topn

set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Tpch_Q03DuckDB
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Tpch
open TinylambOpt.ResearchProofs.BenchmarkRematch.Topn

theorem sound : Q03DuckDB.Goal := by
  intro shards
  exact topn_flatMap 10 shards

end TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Tpch_Q03DuckDB

#print axioms TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Tpch_Q03DuckDB.sound
