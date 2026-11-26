import TinylambOpt.ResearchGoals.BenchmarkRematch.Analytics
import TinylambOpt.ResearchProofs.BenchmarkRematch.Group

set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Analytics_C2DuckDB
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Analytics
open TinylambOpt.ResearchProofs.BenchmarkRematch.Group

theorem sound : C2DuckDB.Goal := by
  intro commenters eligible
  have h := group_sum_nat (fun k => eligible.count k) commenters.length commenters (Nat.le_refl _)
  rw [← h, List.length_flatMap]
  congr 1
  apply List.map_congr_left
  intro k _
  simp [List.count_eq_length_filter]

end TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Analytics_C2DuckDB

#print axioms TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Analytics_C2DuckDB.sound
