import TinylambOpt.ResearchGoals.BenchmarkRematch.Tpch

set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Tpch_Q08DuckDB
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Tpch

theorem sound : Q08DuckDB.Goal := by
  intro rows
  induction rows with
  | nil => simp [total]
  | cons r rows ih =>
    obtain ⟨f, v⟩ := r
    cases f <;> simp [total, List.filter_cons] at ih ⊢ <;> omega

end TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Tpch_Q08DuckDB

#print axioms TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Tpch_Q08DuckDB.sound
