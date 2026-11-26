import TinylambOpt.ResearchGoals.BenchmarkRematch.Click

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Click_Q18ClickHouse
open TinylambOpt.ResearchGoals.BenchmarkRematch.Click

theorem sound : Q18ClickHouse.Goal := by
  intro xs
  dsimp only
  refine List.map_congr_left (fun k hk => ?_)
  have hc : ((xs.eraseDups.take 10).contains k) = true := by simpa using hk
  rw [List.count_eq_countP, List.countP_filter]
  apply List.countP_congr
  intro x _
  by_cases hx : x == k
  · have : x = k := by simpa using hx
    subst this
    simp [hk]
  · simp [hx]

end TinylambOpt.ResearchProofs.BenchmarkRematch.Click_Q18ClickHouse
