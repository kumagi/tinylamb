import TinylambOpt.ProposedGoals.BenchmarkRematch.ClickBench

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.ProposedGoals.ClickBench.Goal_Q03_BlockFooterExactSumCountReuseGoal

open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ProposedGoals.BenchmarkRematch.ClickBench

theorem sound : Q03_BlockFooterExactSumCountReuseGoal := by
  intro blocks
  induction blocks with
  | nil => simp [total]
  | cons b bs ih =>
      simp only [List.flatten_cons, List.map_cons, List.length_append, List.sum_cons,
        List.map_append, List.sum_append, total] at ih ⊢
      exact congrArg (fun z : Nat × Int × Int =>
        (b.length + z.1, (b.map Prod.fst).sum + z.2.1, (b.map Prod.snd).sum + z.2.2)) ih

end TinylambOpt.ResearchProofs.ProposedGoals.ClickBench.Goal_Q03_BlockFooterExactSumCountReuseGoal
