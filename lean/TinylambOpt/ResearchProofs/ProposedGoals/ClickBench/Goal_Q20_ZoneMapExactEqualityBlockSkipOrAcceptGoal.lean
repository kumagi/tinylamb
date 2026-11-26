import TinylambOpt.ProposedGoals.BenchmarkRematch.ClickBench

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.ProposedGoals.ClickBench.Goal_Q20_ZoneMapExactEqualityBlockSkipOrAcceptGoal

open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ProposedGoals.BenchmarkRematch.ClickBench

theorem sound : Q20_ZoneMapExactEqualityBlockSkipOrAcceptGoal := by
  intro xs lo hi target hbounds houtside
  simp only [countWhere]
  have hfilter : xs.filter (fun x => x == target) = [] := by
    apply List.filter_eq_nil_iff.mpr
    intro x hx hxt
    have hxt' : x = target := by simpa using hxt
    have hxlo := (hbounds x hx).1
    have hxhi := (hbounds x hx).2
    omega
  simp [hfilter]

end TinylambOpt.ResearchProofs.ProposedGoals.ClickBench.Goal_Q20_ZoneMapExactEqualityBlockSkipOrAcceptGoal
