import TinylambOpt.ResearchGoals.BenchmarkRematch.Tpch

set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Tpch_Q01DuckDB
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Tpch

theorem sound : Q01DuckDB.Goal := by
  intro rows
  dsimp only
  induction rows with
  | nil => simp [total]
  | cons r rows ih =>
    obtain ⟨p, d, t⟩ := r
    simp only [total, List.map_cons, List.sum_cons] at ih ⊢
    simp only [List.map_map, Function.comp_def] at ih ⊢


end TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Tpch_Q01DuckDB

#print axioms TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Tpch_Q01DuckDB.sound
