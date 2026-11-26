import TinylambOpt.ResearchGoals.JitFrontiers.PredicateBallots

/-!
Vector predicates use lane masks to carry the SQL truth or selection state into later
operators.

Proved

- `ConditionalNull.UnknownSuppresses` — `test.scalar .null .fault=.null`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Goal_ConditionalNull_UnknownSuppresses

open TinylambOpt.ResearchGoals.JitFrontiers.PredicateBallots

theorem sound : ConditionalNull.UnknownSuppresses.Goal := by
  unfold ConditionalNull.UnknownSuppresses.Goal
  native_decide

end TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Goal_ConditionalNull_UnknownSuppresses
