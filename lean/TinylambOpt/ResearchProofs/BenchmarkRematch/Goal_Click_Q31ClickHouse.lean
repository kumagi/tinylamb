import TinylambOpt.ResearchGoals.BenchmarkRematch.Click

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Click_Q31ClickHouse
open TinylambOpt.ResearchGoals.BenchmarkRematch.Click

theorem sound : Q31ClickHouse.Goal := by
  intro pairs e ip
  simp only [List.count_eq_countP, List.countP_map, List.countP_filter]
  apply List.countP_congr
  intro p _
  cases p with
  | mk a b => simp [Prod.ext_iff, and_comm]

end TinylambOpt.ResearchProofs.BenchmarkRematch.Click_Q31ClickHouse
