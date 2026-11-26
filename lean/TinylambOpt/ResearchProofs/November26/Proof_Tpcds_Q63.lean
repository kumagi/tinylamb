import TinylambOpt.ResearchGoals.November26.Tpcds

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.November26.Tpcds.Goal_Q63
open TinylambOpt.ResearchGoals.BenchmarkRematch

theorem sound : TinylambOpt.ResearchGoals.November26.Tpcds.Q63.Goal := by
  intro facts a b price
  unfold total
  congr 1
  apply congrArg (List.map price)
  apply List.filter_congr
  intro k hk
  simp [List.contains_eq_mem]
end TinylambOpt.ResearchProofs.November26.Tpcds.Goal_Q63
