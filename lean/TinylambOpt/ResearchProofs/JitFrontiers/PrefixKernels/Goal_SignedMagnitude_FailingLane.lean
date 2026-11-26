import TinylambOpt.ResearchGoals.JitFrontiers.PrefixKernels
import TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Common_PrefixKernels

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Goal_SignedMagnitude_FailingLane

open TinylambOpt.ResearchGoals.JitFrontiers.PrefixKernels

theorem sound : SignedMagnitude.FailingLane.Goal := by
  exact gen_failingLane SignedMagnitude.update

end TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Goal_SignedMagnitude_FailingLane
