import TinylambOpt.ResearchGoals.BenchmarkRematch.Analytics
import TinylambOpt.ResearchProofs.BenchmarkRematch.Group
import TinylambOpt.ResearchProofs.BenchmarkRematch.Nsum

set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Analytics_C1DuckDB
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Analytics
open TinylambOpt.ResearchProofs.BenchmarkRematch.Group
open TinylambOpt.ResearchProofs.BenchmarkRematch.Nsum

theorem sound : C1DuckDB.Goal := by
  intro scores votes
  dsimp only
  refine Prod.ext ?_ ?_
  · exact flatMap_rep_length scores votes
  · exact nsum_rep_flat scores votes

end TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Analytics_C1DuckDB

#print axioms TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Analytics_C1DuckDB.sound
