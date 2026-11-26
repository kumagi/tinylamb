import TinylambOpt.ResearchGoals.BenchmarkRematch.Click

set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Click.Goal_Q40DuckDB
open TinylambOpt.ResearchGoals.BenchmarkRematch.Click

theorem sound : TinylambOpt.ResearchGoals.BenchmarkRematch.Click.Q40DuckDB.Goal := by
  intro rows referrer
  induction rows with
  | nil => simp
  | cons r rs ih =>
      rcases r with ⟨s, a, i⟩
      simp [ih]

end TinylambOpt.ResearchProofs.BenchmarkRematch.Click.Goal_Q40DuckDB
