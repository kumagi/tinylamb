import TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Common_BailoutState

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Goal_MinValid_DominatedPayload

open TinylambOpt.ResearchGoals.JitFrontiers.BailoutState
open TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Common


theorem sound : MinValid.DominatedPayload.Goal := by
  intro s k x y hxy
  rw [refRun_cons, refRun_cons]
  simp [refRun]
  have hk1 : MinValid.policy.key (k, some x) = some k := by simp [MinValid.policy]
  have hk2 : MinValid.policy.key (k, some y) = some k := by simp [MinValid.policy]
  rw [lookup_step_same MinValid.policy (step MinValid.policy s (k, some x)) (k, some y) k hk2]
  rw [lookup_step_same MinValid.policy s (k, some x) k hk1]
  simp [MinValid.policy]
  omega

end TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Goal_MinValid_DominatedPayload
