import TinylambOpt.ResearchGoals.JitFrontiers.PrefixKernels
import TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Common_PrefixKernels

/-!
Prefix computations can use vector or block summaries when their carried state produces
the same per-row results.

Proved

- `SignedSum.ExactEntryGuard` — Intersect inverse affine ranges to characterize every
  valid incoming accumulator.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Goal_SignedSum_ExactEntryGuard

open TinylambOpt.ResearchGoals.JitFrontiers.PrefixKernels

theorem sound : SignedSum.ExactEntryGuard.Goal := by
  exact gen_exactEntryGuard SignedSum.update

end TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Goal_SignedSum_ExactEntryGuard
