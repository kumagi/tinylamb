import TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Common_BailoutState

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Goal_LastValue_ReserveExactly

open TinylambOpt.ResearchGoals.JitFrontiers.BailoutState
open TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Common

theorem sound : LastValue.ReserveExactly.Goal := by
  exact reserveExactly_of LastValue.policy

end TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Goal_LastValue_ReserveExactly
