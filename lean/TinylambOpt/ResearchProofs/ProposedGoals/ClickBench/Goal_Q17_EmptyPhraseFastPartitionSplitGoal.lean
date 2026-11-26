import TinylambOpt.ProposedGoals.BenchmarkRematch.ClickBench

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.ProposedGoals.ClickBench.Goal_Q17_EmptyPhraseFastPartitionSplitGoal

open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ProposedGoals.BenchmarkRematch.ClickBench

theorem sound : Q17_EmptyPhraseFastPartitionSplitGoal := by
  intro rows u
  simp [countWhere, List.filter_filter]

end TinylambOpt.ResearchProofs.ProposedGoals.ClickBench.Goal_Q17_EmptyPhraseFastPartitionSplitGoal
