import TinylambOpt.ResearchGoals.JitFrontiers.PredicateBallots

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Goal_Implication_FaultPremise

open TinylambOpt.ResearchGoals.JitFrontiers.PredicateBallots

theorem sound : Implication.FaultPremise.Goal := by
  intro rhs
  cases rhs <;> native_decide

end TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Goal_Implication_FaultPremise
