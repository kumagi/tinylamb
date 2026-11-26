import TinylambOpt.ResearchGoals.JitFrontiers.PrefixKernels
import TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Common_PrefixKernels

/-!
Prefix computations can use vector or block summaries when their carried state produces
the same per-row results.

Proved

- `AlternatingDifference.UncheckedFold` — A verified block requires only a multiply-add
  on entry to compute its outgoing state.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Goal_AlternatingDifference_UncheckedFold

open TinylambOpt.ResearchGoals.JitFrontiers.PrefixKernels

theorem sound : AlternatingDifference.UncheckedFold.Goal := by
  exact gen_uncheckedFold AlternatingDifference.update

end TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Goal_AlternatingDifference_UncheckedFold
