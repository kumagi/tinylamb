import TinylambOpt.ResearchGoals.BenchmarkRematch.Click

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Click_Q24DuckDB
open TinylambOpt.ResearchGoals.BenchmarkRematch.Click

theorem sound : Q24DuckDB.Goal := by
  intro ids key payload
  dsimp only
  have h1 : ((ids.map fun i => (i, payload i))).mergeSort (fun a b => decide (key a.1 ≤ key b.1))
      = (ids.mergeSort (fun a b => decide (key a ≤ key b))).map (fun i => (i, payload i)) := by
    symm
    refine List.map_mergeSort ?_
    intro a ha b hb
    rfl
  rw [h1, ← List.map_take, List.map_map]
  refine List.map_congr_left (fun i _ => rfl)

end TinylambOpt.ResearchProofs.BenchmarkRematch.Click_Q24DuckDB
