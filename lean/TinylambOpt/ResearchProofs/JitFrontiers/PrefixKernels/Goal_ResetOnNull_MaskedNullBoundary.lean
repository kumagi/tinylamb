import TinylambOpt.ResearchGoals.JitFrontiers.PrefixKernels
import TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Common_PrefixKernels

/-!
Prefix computations can use vector or block summaries when their carried state produces
the same per-row results.

Proved

- `ResetOnNull.MaskedNullBoundary` — `execute update 8 0 5 [none]=.ok 0`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Goal_ResetOnNull_MaskedNullBoundary

open TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels
open TinylambOpt.ResearchGoals.JitFrontiers.PrefixKernels
open TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites (fits limit)

theorem sound : ResetOnNull.MaskedNullBoundary.Goal := by
  unfold ResetOnNull.MaskedNullBoundary.Goal
  change execute ResetOnNull.update 8 0 5 [none] = Except.ok 0
  unfold execute
  simp [ResetOnNull.update, apply, fits_zero, execute]


end TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Goal_ResetOnNull_MaskedNullBoundary
