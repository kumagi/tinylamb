import TinylambOpt.ResearchGoals.BenchmarkRematch.Click

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Click_Q23DuckDB
open TinylambOpt.ResearchGoals.BenchmarkRematch.Click

theorem sound : Q23DuckDB.Goal := by
  intro xs
  rw [List.filter_filter]
  refine List.filter_congr (fun x _ => ?_)
  obtain ⟨p, t, u⟩ := x
  by_cases h1 : (p != "") <;> by_cases h2 : (t.contains "Google") <;>
    by_cases h3 : (u.contains ".google.") <;>
    simp [h1, h2, h3, Bool.false_eq_true, ↓reduceIte]

end TinylambOpt.ResearchProofs.BenchmarkRematch.Click_Q23DuckDB
