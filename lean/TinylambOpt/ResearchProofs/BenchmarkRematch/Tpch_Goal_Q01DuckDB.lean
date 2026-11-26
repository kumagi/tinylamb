import TinylambOpt.ResearchGoals.BenchmarkRematch.Tpch

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Tpch.Goal_Q01DuckDB

open TinylambOpt.ResearchGoals.BenchmarkRematch

theorem sound : TinylambOpt.ResearchGoals.BenchmarkRematch.Tpch.Q01DuckDB.Goal := by
  intro rows
  dsimp only
  apply Prod.ext <;> simp [total, List.map_map, Function.comp_def]

end TinylambOpt.ResearchProofs.BenchmarkRematch.Tpch.Goal_Q01DuckDB

#print axioms TinylambOpt.ResearchProofs.BenchmarkRematch.Tpch.Goal_Q01DuckDB.sound
