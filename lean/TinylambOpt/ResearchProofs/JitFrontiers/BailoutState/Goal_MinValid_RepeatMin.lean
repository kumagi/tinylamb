import TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Common_BailoutState

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Goal_MinValid_RepeatMin

open TinylambOpt.ResearchGoals.JitFrontiers.BailoutState
open TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Common


theorem sound : MinValid.RepeatMin.Goal := by
  intro s r
  cases r with
  | mk k v =>
      cases v with
      | none => simp [step, MinValid.policy]
      | some x => simp [step, MinValid.policy]

end TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Goal_MinValid_RepeatMin
