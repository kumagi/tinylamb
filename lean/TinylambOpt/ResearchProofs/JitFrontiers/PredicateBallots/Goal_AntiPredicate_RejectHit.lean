import TinylambOpt.ResearchGoals.JitFrontiers.PredicateBallots

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Goal_AntiPredicate_RejectHit

open TinylambOpt.ResearchGoals.JitFrontiers.PredicateBallots

theorem sound : AntiPredicate.RejectHit.Goal := by
  unfold AntiPredicate.RejectHit.Goal
  native_decide

end TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Goal_AntiPredicate_RejectHit
