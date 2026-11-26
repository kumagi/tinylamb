import TinylambOpt.ResearchGoals.JitFrontiers.PrefixKernels
import TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Common_PrefixKernels

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Goal_Product_JoinCertificates

open TinylambOpt.ResearchGoals.JitFrontiers.PrefixKernels

theorem sound : Product.JoinCertificates.Goal := by
  exact gen_joinCertificates Product.update

end TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Goal_Product_JoinCertificates
