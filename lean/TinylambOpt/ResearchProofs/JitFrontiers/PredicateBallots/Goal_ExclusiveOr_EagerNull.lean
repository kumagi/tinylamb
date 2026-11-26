import TinylambOpt.ResearchGoals.JitFrontiers.PredicateBallots

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Goal_ExclusiveOr_EagerNull

open TinylambOpt.ResearchGoals.JitFrontiers.PredicateBallots

theorem sound : ExclusiveOr.EagerNull.Goal := by
  unfold ExclusiveOr.EagerNull.Goal
  native_decide

end TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Goal_ExclusiveOr_EagerNull
