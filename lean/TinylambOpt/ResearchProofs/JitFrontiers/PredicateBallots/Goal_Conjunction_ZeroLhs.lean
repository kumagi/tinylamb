import TinylambOpt.ResearchGoals.JitFrontiers.PredicateBallots

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Goal_Conjunction_ZeroLhs

open TinylambOpt.ResearchGoals.JitFrontiers.PredicateBallots

theorem sound : Conjunction.ZeroLhs.Goal := by
  intro b hb
  simp [Conjunction.test, andBits]

end TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Goal_Conjunction_ZeroLhs
