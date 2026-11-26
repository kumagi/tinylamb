import TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Common_BailoutState

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Goal_Packed32_FirstWide

open TinylambOpt.ResearchGoals.JitFrontiers.BailoutState
open TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Common


theorem sound : Packed32.FirstWide.Goal := by
  intro s v cap
  simp [run, drive, canCommit, Packed32.policy]

end TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Goal_Packed32_FirstWide
