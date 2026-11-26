import TinylambOpt.ResearchGoals.BenchmarkRematch.Click

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Click_Q43DuckDB
open TinylambOpt.ResearchGoals.BenchmarkRematch.Click
open TinylambOpt.ResearchGoals.BenchmarkRematch

theorem sound : Q43DuckDB.Goal := by
  intro minutes
  dsimp only
  refine List.map_congr_left (fun m hm => ?_)
  have hc : (ranges minutes.eraseDups 1000 10).contains m = true := by simpa using hm
  rw [List.count_eq_countP, List.countP_filter]
  apply List.countP_congr
  intro x _
  by_cases hx : x == m
  · have : x = m := by simpa using hx
    subst this
    simp [hm]
  · simp [hx]

end TinylambOpt.ResearchProofs.BenchmarkRematch.Click_Q43DuckDB
