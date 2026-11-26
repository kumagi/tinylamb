import TinylambOpt.ResearchGoals.DatabaseCorrectness.SortRetry
import TinylambOpt.ResearchProofs.DatabaseCorrectness.SortRetryCommon

/-!
A failed intermediate merge pass may restart from the already published prefix
of completed passes. Re-sorting each completed group does not change the
multiset or the relative order of equal-key occurrences, so the final stable
stream is the same as sorting the original runs in one pass.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_SortRetry_RetriedMergePassRetainsStableStream

open TinylambOpt.ResearchGoals.DatabaseCorrectness.SortRetry

theorem sound : RetriedMergePassRetainsStableStream.Goal := by
  intro groups rest
  exact TinylambOpt.ResearchProofs.DatabaseCorrectness.SortRetryCommon.published_sorted_eq groups rest

end TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_SortRetry_RetriedMergePassRetainsStableStream
