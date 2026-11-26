import TinylambOpt.ResearchGoals.ResourcePolicies.AggregateReplay

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
Refutes `LateDistinctAfterLocalLimit`: truncating a run to its k best rows before the
final DISTINCT discards duplicate-heavy prefixes, so distinct values that would rank
inside the global top-k after deduplication never reach the final merge. Witness:
k = 2 and the single run [1,1,1,2,3,4]: locally only [1,1] survives, so the left side
is [1], while the right side deduplicates first and yields [1,2]. -/
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_AggregateReplay_LateDistinctAfterLocalLimit
open TinylambOpt.ResearchGoals.ResourcePolicies.AggregateReplay

theorem refute : ¬ LateDistinctAfterLocalLimit.Goal := by
  intro h
  exact absurd (h 2 [[1, 1, 1, 2, 3, 4]]) (by native_decide)

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_AggregateReplay_LateDistinctAfterLocalLimit
