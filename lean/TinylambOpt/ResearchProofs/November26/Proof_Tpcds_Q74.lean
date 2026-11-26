import TinylambOpt.ResearchGoals.November26.Tpcds

set_option maxHeartbeats 0
set_option maxRecDepth 10000


namespace TinylambOpt.ResearchProofs.November26.Tpcds.Goal_Q74
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.November26
open TinylambOpt.ResearchGoals.November26.Tpcds

theorem sound : TinylambOpt.ResearchGoals.November26.Tpcds.Q74.Goal := by
  intro ordered pass cut h
  have hlen : 100 ≤ ((ordered.take cut).filter pass).length := by
    simpa [countWhere] using h
  conv =>
    rhs
    rw [← List.take_append_drop cut ordered]
  rw [List.filter_append, List.take_append_of_le_length hlen]
end TinylambOpt.ResearchProofs.November26.Tpcds.Goal_Q74
