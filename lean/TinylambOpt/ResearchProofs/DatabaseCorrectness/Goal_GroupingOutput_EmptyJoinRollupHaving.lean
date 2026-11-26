import TinylambOpt.ResearchGoals.DatabaseCorrectness.GroupingOutput

/-! An empty completed join still produces the single empty grouping-set row,
    so HAVING is evaluated on that row rather than skipped. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.GroupingOutput.Goal_EmptyJoinRollupHaving

open TinylambOpt.ResearchGoals.DatabaseCorrectness.GroupingOutput
open TinylambOpt.ResearchGoals.QueryRewrites (nsum)

theorem sound : EmptyJoinRollupHaving.Goal := by
  intro having
  by_cases h : having ⟨true, none, 0, none⟩ <;> simp [rollup, grand, nsum, h]

end TinylambOpt.ResearchProofs.DatabaseCorrectness.GroupingOutput.Goal_EmptyJoinRollupHaving
