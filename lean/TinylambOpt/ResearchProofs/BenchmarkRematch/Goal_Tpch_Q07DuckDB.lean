import TinylambOpt.ResearchGoals.BenchmarkRematch.Tpch

set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Tpch_Q07DuckDB
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Tpch

theorem sound : Q07DuckDB.Goal := by
  intro rows a b hab
  apply List.filter_congr
  intro x _
  obtain ⟨s, c⟩ := x
  simp only [List.contains_cons, List.contains_nil, Bool.or_false]
  rw [Bool.eq_iff_iff]
  simp only [Bool.and_eq_true, Bool.or_eq_true, beq_iff_eq, bne_iff_ne, ne_eq]
  omega

end TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Tpch_Q07DuckDB

#print axioms TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Tpch_Q07DuckDB.sound
