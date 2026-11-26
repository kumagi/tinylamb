import TinylambOpt.ResearchGoals.JitFrontiers.PrefixKernels
import TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Common_PrefixKernels

/-!
Prefix computations can use vector or block summaries when their carried state produces
the same per-row results.

Proved

- `PositiveFilter.FailingLane` — Map the first violated prefix certificate back to the
  exact source row.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Goal_PositiveFilter_FailingLane

open TinylambOpt.ResearchGoals.JitFrontiers.PrefixKernels

theorem sound : PositiveFilter.FailingLane.Goal := by
  exact gen_failingLane PositiveFilter.update

end TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Goal_PositiveFilter_FailingLane
