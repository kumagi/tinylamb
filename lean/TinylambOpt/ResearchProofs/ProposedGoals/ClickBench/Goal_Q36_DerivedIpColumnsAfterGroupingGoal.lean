import TinylambOpt.ProposedGoals.BenchmarkRematch.ClickBench

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.ProposedGoals.ClickBench.Goal_Q36_DerivedIpColumnsAfterGroupingGoal

open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ProposedGoals.BenchmarkRematch.ClickBench

theorem sound : Q36_DerivedIpColumnsAfterGroupingGoal := by
  intro ips k
  simp only [countWhere]
  apply congrArg List.length
  apply List.filter_congr
  intro x hx
  by_cases hxk : x == k
  · have heq : x = k := by simpa using hxk
    subst x
    simp
  · have hxkf : (x == k) = false := by simpa using hxk
    simp [hxkf]

end TinylambOpt.ResearchProofs.ProposedGoals.ClickBench.Goal_Q36_DerivedIpColumnsAfterGroupingGoal
