import TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Common_BailoutState

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Goal_ChangedOnly_ZeroSkip

open TinylambOpt.ResearchGoals.JitFrontiers.BailoutState
open TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Common


theorem sound : ChangedOnly.ZeroSkip.Goal := by
  intro k s
  simp [step, ChangedOnly.policy]

end TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Goal_ChangedOnly_ZeroSkip
