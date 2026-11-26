import TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

set_option maxHeartbeats 0
namespace Proof_Q35DuckDB

theorem sound : TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds.Q35DuckDB.Goal := by
  intro v n hn
  cases v with
  | none => simp
  | some x => simp [hn]

end Proof_Q35DuckDB

#print axioms Proof_Q35DuckDB.sound
