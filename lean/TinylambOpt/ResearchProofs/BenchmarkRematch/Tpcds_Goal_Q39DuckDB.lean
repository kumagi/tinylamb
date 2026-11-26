import TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

set_option maxHeartbeats 0
namespace Proof_Q39DuckDB

theorem sound : TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds.Q39DuckDB.Goal := by
  intro xs m hm
  rcases hm with rfl | rfl
  all_goals
    rw [List.filter_filter]
    apply List.filter_congr
    intro r hr
    by_cases h1 : (r.1 == 1) = true
    · simp [h1]
    · by_cases h2 : (r.1 == 2) = true
      · simp [h1, h2]
      · simp [h1, h2]

end Proof_Q39DuckDB

#print axioms Proof_Q39DuckDB.sound
