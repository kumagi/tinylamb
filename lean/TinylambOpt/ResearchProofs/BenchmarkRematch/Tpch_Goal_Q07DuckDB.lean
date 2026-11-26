import TinylambOpt.ResearchGoals.BenchmarkRematch.Tpch

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Tpch.Goal_Q07DuckDB

open TinylambOpt.ResearchGoals.BenchmarkRematch

theorem sound : TinylambOpt.ResearchGoals.BenchmarkRematch.Tpch.Q07DuckDB.Goal := by
  intro rows a b hab
  induction rows with
  | nil => simp
  | cons r rs ih =>
    simp only [List.filter_cons]
    rcases r with ⟨s,c⟩
    simp only
    by_cases hs_a : s == a <;> by_cases hs_b : s == b <;>
      by_cases hc_a : c == a <;> by_cases hc_b : c == b <;> simp_all

end TinylambOpt.ResearchProofs.BenchmarkRematch.Tpch.Goal_Q07DuckDB

#print axioms TinylambOpt.ResearchProofs.BenchmarkRematch.Tpch.Goal_Q07DuckDB.sound
