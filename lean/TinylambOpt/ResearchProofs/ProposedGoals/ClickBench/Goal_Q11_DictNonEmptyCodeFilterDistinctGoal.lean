import TinylambOpt.ProposedGoals.BenchmarkRematch.ClickBench

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.ProposedGoals.ClickBench.Goal_Q11_DictNonEmptyCodeFilterDistinctGoal

open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ProposedGoals.BenchmarkRematch.ClickBench

theorem sound : Q11_DictNonEmptyCodeFilterDistinctGoal := by
  intro codes emptyCode m hm
  have hfilters :
      codes.filter (fun r => (r.1 != emptyCode) && (r.1 == m)) =
        codes.filter (fun r => r.1 == m) := by
    apply List.filter_congr
    intro r hr
    by_cases hrm : r.1 == m
    · have heq : r.1 = m := by simpa using hrm
      subst heq
      simp [hm]
    · simp [hrm]
  rw [hfilters]

end TinylambOpt.ResearchProofs.ProposedGoals.ClickBench.Goal_Q11_DictNonEmptyCodeFilterDistinctGoal
