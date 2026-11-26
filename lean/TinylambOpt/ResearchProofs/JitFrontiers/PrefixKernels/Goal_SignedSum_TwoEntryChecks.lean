import TinylambOpt.ResearchGoals.JitFrontiers.PrefixKernels
import TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Common_PrefixKernels

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Goal_SignedSum_TwoEntryChecks

open TinylambOpt.ResearchGoals.JitFrontiers.PrefixKernels

theorem sound : SignedSum.TwoEntryChecks.Goal := by
  exact gen_twoEntryChecks SignedSum.update

end TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Goal_SignedSum_TwoEntryChecks
