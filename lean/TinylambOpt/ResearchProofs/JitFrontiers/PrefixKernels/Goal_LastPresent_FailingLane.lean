import TinylambOpt.ResearchGoals.JitFrontiers.PrefixKernels
import TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Common_PrefixKernels

/-!
Prefix computations can use vector or block summaries when their carried state produces
the same per-row results.

Proved

- `LastPresent.FailingLane` — Map the first violated prefix certificate back to the
  exact source row.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Goal_LastPresent_FailingLane

open TinylambOpt.ResearchGoals.JitFrontiers.PrefixKernels

theorem sound : LastPresent.FailingLane.Goal := by
  exact gen_failingLane LastPresent.update

end TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Goal_LastPresent_FailingLane
