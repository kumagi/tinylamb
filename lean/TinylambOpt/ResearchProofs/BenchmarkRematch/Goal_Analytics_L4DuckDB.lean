import TinylambOpt.ResearchGoals.BenchmarkRematch.Analytics
import TinylambOpt.ResearchProofs.BenchmarkRematch.Group
import TinylambOpt.ResearchProofs.BenchmarkRematch.Nsum

set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Analytics_L4DuckDB
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Analytics
open TinylambOpt.ResearchProofs.BenchmarkRematch.Group
open TinylambOpt.ResearchProofs.BenchmarkRematch.Nsum

theorem sound : L4DuckDB.Goal := by
  intro scores c
  dsimp only
  have h1 := fm_flatten_rep scores c
  refine Prod.ext ?_ (Prod.ext ?_ ?_)
  · exact flatten_rep_length scores c
  · show (((List.replicate c scores).flatten).filterMap id).sum = _
    rw [h1, flatten_rep_sum]
  · show (((List.replicate c scores).flatten).filterMap id).length = _
    rw [h1, flatten_rep_length]

end TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Analytics_L4DuckDB

#print axioms TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Analytics_L4DuckDB.sound
