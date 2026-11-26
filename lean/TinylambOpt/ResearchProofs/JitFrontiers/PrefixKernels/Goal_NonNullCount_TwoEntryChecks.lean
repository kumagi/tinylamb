import TinylambOpt.ResearchGoals.JitFrontiers.PrefixKernels
import TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Common_PrefixKernels

/-!
Prefix computations can use vector or block summaries when their carried state produces
the same per-row results.

Proved

- `NonNullCount.TwoEntryChecks` — Safe endpoints certify a full interval of possible
  initial states.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Goal_NonNullCount_TwoEntryChecks

open TinylambOpt.ResearchGoals.JitFrontiers.PrefixKernels

theorem sound : NonNullCount.TwoEntryChecks.Goal := by
  exact gen_twoEntryChecks NonNullCount.update

end TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Goal_NonNullCount_TwoEntryChecks
