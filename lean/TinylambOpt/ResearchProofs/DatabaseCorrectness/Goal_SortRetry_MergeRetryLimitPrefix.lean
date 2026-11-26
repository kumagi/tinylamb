import TinylambOpt.ResearchGoals.DatabaseCorrectness.SortRetry
import TinylambOpt.ResearchProofs.DatabaseCorrectness.SortRetryCommon

/-!
LIMIT may discard unread runs as soon as the already emitted prefix is fixed.
Because a restarted merge produces exactly the same sorted stream, dropping
`already` rows and taking `n` gives the same prefix in both executions.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_SortRetry_MergeRetryLimitPrefix

open TinylambOpt.ResearchGoals.DatabaseCorrectness.SortRetry
open TinylambOpt.ResearchProofs.DatabaseCorrectness.SortRetryCommon (published_sorted_eq)

theorem sound : MergeRetryLimitPrefix.Goal := by
  intro groups rest already n
  rw [published_sorted_eq groups rest]

end TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_SortRetry_MergeRetryLimitPrefix
