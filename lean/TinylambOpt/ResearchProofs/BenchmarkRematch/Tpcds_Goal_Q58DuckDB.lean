import TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

set_option maxHeartbeats 0
namespace Proof_Q58DuckDB

theorem sound : TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds.Q58DuckDB.Goal := by
  intro days week date
  apply Bool.eq_iff_iff.mpr
  simp [List.any_eq_true]

end Proof_Q58DuckDB

#print axioms Proof_Q58DuckDB.sound
