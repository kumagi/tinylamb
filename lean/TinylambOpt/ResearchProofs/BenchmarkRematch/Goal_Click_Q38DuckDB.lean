import TinylambOpt.ResearchGoals.BenchmarkRematch.Click

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Click_Q38DuckDB
open TinylambOpt.ResearchGoals.BenchmarkRematch.Click

theorem sound : Q38DuckDB.Goal := by
  intro pages title
  induction pages with
  | nil => rfl
  | cons p t ih =>
    simp only [List.map_cons, List.sum_cons, List.flatten_cons, List.count_append, ih]

end TinylambOpt.ResearchProofs.BenchmarkRematch.Click_Q38DuckDB
