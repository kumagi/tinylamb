import TinylambOpt.ResearchGoals.BenchmarkRematch.Click

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Click_Q40DuckDB
open TinylambOpt.ResearchGoals.BenchmarkRematch.Click

theorem sound : Q40DuckDB.Goal := by
  intro rows referrer
  induction rows with
  | nil => rfl
  | cons r t ih =>
    obtain ⟨s, a, i⟩ := r
    simp

end TinylambOpt.ResearchProofs.BenchmarkRematch.Click_Q40DuckDB
