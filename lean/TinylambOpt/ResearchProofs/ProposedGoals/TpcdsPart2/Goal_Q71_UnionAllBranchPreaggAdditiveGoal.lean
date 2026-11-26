import TinylambOpt.ProposedGoals.BenchmarkRematch.TpcdsPart2

/-!
# Additivity of per-branch pre-aggregation over UNION ALL (TPC-DS Q71)

Filtering and summing each UNION ALL branch with the same key predicate equals filtering
and summing the concatenated branches, since list append distributes through both
operations. The executor may push semi-join bitsets into the three branches and aggregate
each one independently.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.ProposedGoals.TpcdsPart2.Goal_Q71_UnionAllBranchPreaggAdditiveGoal

open TinylambOpt.ProposedGoals.BenchmarkRematch.TpcdsPart2
open TinylambOpt.ResearchGoals.QueryRewrites

theorem sound : Q71_UnionAllBranchPreaggAdditiveGoal := by
  intro b1 b2 b3 k
  simp [total, List.filter_append, List.map_append, List.sum_append, Int.add_assoc]

end TinylambOpt.ResearchProofs.ProposedGoals.TpcdsPart2.Goal_Q71_UnionAllBranchPreaggAdditiveGoal
