import TinylambOpt.ResearchGoals.BenchmarkRematch.Analytics
import TinylambOpt.ResearchProofs.BenchmarkRematch.Group

set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Analytics_B5DuckDB
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Analytics
open TinylambOpt.ResearchProofs.BenchmarkRematch.Group

theorem sound : B5DuckDB.Goal := by
  intro sold priced
  dsimp only
  apply List.filter_congr
  intro x hx
  rw [Bool.eq_iff_iff]
  simp [List.contains_iff_mem, List.mem_filter, List.mem_eraseDups, hx]

end TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Analytics_B5DuckDB

#print axioms TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Analytics_B5DuckDB.sound
