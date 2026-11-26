import TinylambOpt.ResearchGoals.JitFrontiers.PredicateBallots

/-!
Vector predicates use lane masks to carry the SQL truth or selection state into later
operators.

Proved

- `NullSafeEqual.NoUnknown` — `∀ a b, (test.emit a b).nul=0`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Goal_NullSafeEqual_NoUnknown

open TinylambOpt.ResearchGoals.JitFrontiers.PredicateBallots

theorem sound : NullSafeEqual.NoUnknown.Goal := by
  intro a b
  simp [NullSafeEqual.test]

end TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Goal_NullSafeEqual_NoUnknown
