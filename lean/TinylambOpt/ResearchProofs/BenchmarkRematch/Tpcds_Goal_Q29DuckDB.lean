import TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

set_option maxHeartbeats 0
namespace Proof_Q29DuckDB
open TinylambOpt.ResearchGoals.BenchmarkRematch

theorem sound : TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds.Q29DuckDB.Goal := by
  intro catalog tickets
  induction catalog with
  | nil => simp [total]
  | cons r rest ih =>
    by_cases h : (tickets r.1 != 0) = true
    · simpa [total, h] using ih
    · have hz : tickets r.1 = 0 := by
        simp at h
        exact h
      simpa [total, h, hz] using ih

end Proof_Q29DuckDB

#print axioms Proof_Q29DuckDB.sound
