import TinylambOpt.ResearchGoals.JitFrontiers.PredicateBallots

/-!
Vector predicates use lane masks to carry the SQL truth or selection state into later
operators.

Proved

- `NullSafeEqual.EqualNull` — `test.scalar .null .null=.yes`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Goal_NullSafeEqual_EqualNull

open TinylambOpt.ResearchGoals.JitFrontiers.PredicateBallots

theorem sound : NullSafeEqual.EqualNull.Goal := by
  unfold NullSafeEqual.EqualNull.Goal
  native_decide

end TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Goal_NullSafeEqual_EqualNull
