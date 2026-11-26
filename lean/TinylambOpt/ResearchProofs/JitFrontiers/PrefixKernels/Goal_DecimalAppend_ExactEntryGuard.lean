import TinylambOpt.ResearchGoals.JitFrontiers.PrefixKernels
import TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Common_PrefixKernels

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Goal_DecimalAppend_ExactEntryGuard

open TinylambOpt.ResearchGoals.JitFrontiers.PrefixKernels

theorem sound : DecimalAppend.ExactEntryGuard.Goal := by
  exact gen_exactEntryGuard DecimalAppend.update

end TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Goal_DecimalAppend_ExactEntryGuard
