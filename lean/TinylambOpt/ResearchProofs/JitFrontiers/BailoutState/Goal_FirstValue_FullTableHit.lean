import TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Common_BailoutState

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Goal_FirstValue_FullTableHit

open TinylambOpt.ResearchGoals.JitFrontiers.BailoutState
open TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Common

theorem sound : FirstValue.FullTableHit.Goal := by
  exact fullTableHit_of FirstValue.policy

end TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Goal_FirstValue_FullTableHit
