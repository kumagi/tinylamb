import TinylambOpt.ResearchProofs.JitFrontiers.DemandSSA.Common_DemandSSA

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.DemandSSA.Goal_DoubleNegation_NullRemains

open TinylambOpt.ResearchGoals.JitFrontiers.DemandSSA
open TinylambOpt.ResearchProofs.JitFrontiers.DemandSSA.Common

theorem sound : DoubleNegation.NullRemains.Goal := by
  rfl

end TinylambOpt.ResearchProofs.JitFrontiers.DemandSSA.Goal_DoubleNegation_NullRemains
