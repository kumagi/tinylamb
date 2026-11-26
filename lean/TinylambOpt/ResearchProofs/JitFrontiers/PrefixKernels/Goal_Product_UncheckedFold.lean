import TinylambOpt.ResearchGoals.JitFrontiers.PrefixKernels
import TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Common_PrefixKernels

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Goal_Product_UncheckedFold

open TinylambOpt.ResearchGoals.JitFrontiers.PrefixKernels

theorem sound : Product.UncheckedFold.Goal := by
  exact gen_uncheckedFold Product.update

end TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Goal_Product_UncheckedFold
