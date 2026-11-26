import TinylambOpt.ProposedGoals.BenchmarkRematch.ClickBench

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.ProposedGoals.ClickBench.Goal_Q18_UnorderedLimit10FixedKeySetCountGoal

open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ProposedGoals.BenchmarkRematch.ClickBench

private theorem countWhere_append {α : Type} (xs ys : List α) (p : α → Bool) :
    countWhere (xs ++ ys) p = countWhere xs p + countWhere ys p := by
  simp [countWhere]

theorem sound : Q18_UnorderedLimit10FixedKeySetCountGoal := by
  intro prefixRows restRows k hk
  exact countWhere_append prefixRows restRows (fun r => r == k)

end TinylambOpt.ResearchProofs.ProposedGoals.ClickBench.Goal_Q18_UnorderedLimit10FixedKeySetCountGoal
