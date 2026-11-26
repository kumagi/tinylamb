import TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns
import TinylambOpt.ResearchProofs.QueryRewrites.NullableSum

/-!
# DayOfWeekNullablePartialSums

Combining weekday subtotals from both sales channels preserves the nullable sum. The
executor can pass aggregate states through the union.
-/


set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.DayOfWeekNullablePartialSums
open TinylambOpt.ResearchProofs.QueryRewrites.NullableSum (nsum_map_nsum)

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns.DayOfWeekNullablePartialSums.Goal := by
  intro a b
  simpa using nsum_map_nsum [a, b]
