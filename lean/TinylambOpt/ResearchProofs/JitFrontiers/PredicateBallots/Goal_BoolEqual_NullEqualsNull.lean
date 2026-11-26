import TinylambOpt.ResearchGoals.JitFrontiers.PredicateBallots

/-!
Vector predicates use lane masks to carry the SQL truth or selection state into later
operators.

Proved

- `BoolEqual.NullEqualsNull` — `test.scalar .null .null=.null`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Goal_BoolEqual_NullEqualsNull

open TinylambOpt.ResearchGoals.JitFrontiers.PredicateBallots

theorem sound : BoolEqual.NullEqualsNull.Goal := by
  unfold BoolEqual.NullEqualsNull.Goal
  native_decide

end TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Goal_BoolEqual_NullEqualsNull
