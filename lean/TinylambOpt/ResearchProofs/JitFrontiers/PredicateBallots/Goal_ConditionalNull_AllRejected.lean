import TinylambOpt.ResearchGoals.JitFrontiers.PredicateBallots

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Goal_ConditionalNull_AllRejected

open TinylambOpt.ResearchGoals.JitFrontiers.PredicateBallots

theorem sound : ConditionalNull.AllRejected.Goal := by
  intro b
  simp [ConditionalNull.test, zeros]

end TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Goal_ConditionalNull_AllRejected
