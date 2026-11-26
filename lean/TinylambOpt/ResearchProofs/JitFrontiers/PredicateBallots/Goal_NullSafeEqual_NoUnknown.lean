import TinylambOpt.ResearchGoals.JitFrontiers.PredicateBallots

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Goal_NullSafeEqual_NoUnknown

open TinylambOpt.ResearchGoals.JitFrontiers.PredicateBallots

theorem sound : NullSafeEqual.NoUnknown.Goal := by
  intro a b
  simp [NullSafeEqual.test]

end TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Goal_NullSafeEqual_NoUnknown
