import TinylambOpt.ResearchGoals.BenchmarkRematch.Analytics
import TinylambOpt.ResearchProofs.BenchmarkRematch.Group

set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Analytics_J3DuckDB
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Analytics
open TinylambOpt.ResearchProofs.BenchmarkRematch.Group

theorem sound : J3DuckDB.Goal := by
  intro cast names lo hi hc
  dsimp only
  apply List.filter_congr
  intro k hk
  have h := hc k hk
  have e1 : k - lo < hi + 1 - lo := by omega
  have e2 : lo + (k - lo) = k := by omega
  have : (List.map (fun i => names.contains (lo+i)) (List.range (hi+1-lo)))[k-lo]? = some (names.contains k) := by
    rw [List.getElem?_map, List.getElem?_range e1]
    simp [e2]
  rw [this]
  cases names.contains k <;> rfl

end TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Analytics_J3DuckDB

#print axioms TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Analytics_J3DuckDB.sound
