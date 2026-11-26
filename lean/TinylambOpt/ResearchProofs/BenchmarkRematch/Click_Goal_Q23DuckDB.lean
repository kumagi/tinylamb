import TinylambOpt.ResearchGoals.BenchmarkRematch.Click

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Click.Click_Goal_Q23DuckDB
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Click

theorem sound : TinylambOpt.ResearchGoals.BenchmarkRematch.Click.Q23DuckDB.Goal := by
  intro xs
  induction xs with
  | nil => simp
  | cons x xs ih =>
      rcases x with ⟨p, t, u⟩
      simp only [List.filter_cons]
      by_cases hp : (p != "") = true
      · by_cases ht : t.contains "Google" = true
        · by_cases hu : (!u.contains ".google.") = true
          · simp [hp, ht, hu, ih]
          · simp [hp, ht, hu, ih]
        · simp [hp, ht, ih]
      · simp [hp, ih]
end TinylambOpt.ResearchProofs.BenchmarkRematch.Click.Click_Goal_Q23DuckDB
