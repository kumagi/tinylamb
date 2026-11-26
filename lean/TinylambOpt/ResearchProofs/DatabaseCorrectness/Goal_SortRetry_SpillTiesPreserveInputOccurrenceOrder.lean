import TinylambOpt.ResearchGoals.DatabaseCorrectness.SortRetry
import TinylambOpt.ResearchProofs.DatabaseCorrectness.SortRetryCommon

/-!
Sorting on the key alone is stable: rows tied on a key keep their original
occurrence order, even across run boundaries and duplicate row ids. Hence
filtering the spilled-and-sorted stream by one key recovers exactly the input
rows with that key.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_SortRetry_SpillTiesPreserveInputOccurrenceOrder

open TinylambOpt.ResearchGoals.DatabaseCorrectness.SortRetry
open TinylambOpt.ResearchProofs.DatabaseCorrectness.SortRetryCommon (byKey)
open TinylambOpt.ResearchProofs.DatabaseCorrectness.SortRetryCommon (filter_sorted)
open TinylambOpt.ResearchProofs.DatabaseCorrectness.SortRetryCommon (published_filter)

theorem sound : SpillTiesPreserveInputOccurrenceOrder.Goal := by
  intro groups rest k
  change (sorted (published groups rest).flatten).filter (byKey k) =
    ((groups.flatten ++ rest).flatten).filter (byKey k)
  rw [filter_sorted, published_filter]

end TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_SortRetry_SpillTiesPreserveInputOccurrenceOrder
