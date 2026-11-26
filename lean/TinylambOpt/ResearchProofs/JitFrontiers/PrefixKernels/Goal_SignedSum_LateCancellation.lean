import TinylambOpt.ResearchGoals.JitFrontiers.PrefixKernels
import TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Common_PrefixKernels

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Goal_SignedSum_LateCancellation

open TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels
open TinylambOpt.ResearchGoals.JitFrontiers.PrefixKernels
open TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites (fits limit)

theorem sound : SignedSum.LateCancellation.Goal := by
  intro w hw
  have hfits : fits w (limit w - 1) = true := by
    rw [fits_true]
    have hp := limit_pos w
    omega
  have hbad : fits w (limit w) = false := not_fits_limit w
  unfold execute
  simp [SignedSum.update, apply, hfits, hbad]


end TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Goal_SignedSum_LateCancellation
