import TinylambOpt.ResearchGoals.JitFrontiers.PrefixKernels
import TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Common_PrefixKernels

/-!
Prefix computations can use vector or block summaries when their carried state produces
the same per-row results.

Proved

- `Product.MinNegation` — `∀ w, execute update w 0 (-limit w) [some (-1)]=.error 0`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Goal_Product_MinNegation

open TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels
open TinylambOpt.ResearchGoals.JitFrontiers.PrefixKernels
open TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites (fits limit)

theorem sound : Product.MinNegation.Goal := by
  intro w
  have hbad : fits w (limit w) = false := not_fits_limit w
  unfold execute
  simp [Product.update, apply, hbad]


end TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Goal_Product_MinNegation
