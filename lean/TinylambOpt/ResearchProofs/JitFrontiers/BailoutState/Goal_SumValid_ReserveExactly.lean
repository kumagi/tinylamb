import TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Common_BailoutState

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Goal_SumValid_ReserveExactly

open TinylambOpt.ResearchGoals.JitFrontiers.BailoutState
open TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Common

theorem sound : SumValid.ReserveExactly.Goal := by
  exact reserveExactly_of SumValid.policy

end TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Goal_SumValid_ReserveExactly
