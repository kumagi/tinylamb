import TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

set_option maxHeartbeats 0
namespace Proof_Q62DuckDB
open TinylambOpt.ResearchGoals.BenchmarkRematch

theorem sound : TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds.Q62DuckDB.Goal := by
  intro d
  dsimp only
  by_cases h1 : 30 < d <;> by_cases h2 : 60 < d <;>
  by_cases h3 : 90 < d <;> by_cases h4 : 120 < d <;>
  simp [bucket, h1, h2, h3, h4] <;> omega

end Proof_Q62DuckDB

#print axioms Proof_Q62DuckDB.sound
