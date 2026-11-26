import TinylambOpt.ResearchGoals.JitFrontiers.PredicateBallots

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Goal_Disjunction_TrueSuppresses

open TinylambOpt.ResearchGoals.JitFrontiers.PredicateBallots

theorem sound : Disjunction.TrueSuppresses.Goal := by
  intro b hb
  simp [Disjunction.test, andBits, negBits, zeros]

end TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Goal_Disjunction_TrueSuppresses
