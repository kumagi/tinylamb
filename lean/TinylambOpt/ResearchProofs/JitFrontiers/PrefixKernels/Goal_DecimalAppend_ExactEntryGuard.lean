import TinylambOpt.ResearchGoals.JitFrontiers.PrefixKernels
import TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Common_PrefixKernels

/-!
Prefix computations can use vector or block summaries when their carried state produces
the same per-row results.

Proved

- `DecimalAppend.ExactEntryGuard` — Intersect inverse affine ranges to characterize
  every valid incoming accumulator.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Goal_DecimalAppend_ExactEntryGuard

open TinylambOpt.ResearchGoals.JitFrontiers.PrefixKernels

theorem sound : DecimalAppend.ExactEntryGuard.Goal := by
  exact gen_exactEntryGuard DecimalAppend.update

end TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Goal_DecimalAppend_ExactEntryGuard
