import TinylambOpt.ResearchGoals.JitFrontiers.PrefixKernels
import TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Common_PrefixKernels

/-!
Prefix computations can use vector or block summaries when their carried state produces
the same per-row results.

Proved

- `DifferenceFromBase.MergeOrder` — Reordering arbitrary partitions is safe when the
  composed maps commute and both orders pass prefix checks.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Goal_DifferenceFromBase_MergeOrder

open TinylambOpt.ResearchGoals.JitFrontiers.PrefixKernels

theorem sound : DifferenceFromBase.MergeOrder.Goal := by
  exact gen_mergeOrder DifferenceFromBase.update

end TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Goal_DifferenceFromBase_MergeOrder
