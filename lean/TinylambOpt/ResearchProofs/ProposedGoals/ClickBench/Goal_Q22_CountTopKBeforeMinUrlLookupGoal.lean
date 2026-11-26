import TinylambOpt.ProposedGoals.BenchmarkRematch.ClickBench

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.ProposedGoals.ClickBench.Goal_Q22_CountTopKBeforeMinUrlLookupGoal

open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ProposedGoals.BenchmarkRematch.ClickBench

theorem sound : Q22_CountTopKBeforeMinUrlLookupGoal := by
  intro rows topPhrases p hp
  unfold forKey
  rw [List.filter_filter]
  apply congrArg (List.map Prod.snd)
  apply List.filter_congr
  intro r hr
  by_cases hrp : r.1 == p
  · have heq : r.1 = p := by simpa using hrp
    have hmem : r.1 ∈ topPhrases := by simpa [heq] using hp
    simpa [hrp] using hmem
  · simp [hrp]

end TinylambOpt.ResearchProofs.ProposedGoals.ClickBench.Goal_Q22_CountTopKBeforeMinUrlLookupGoal
