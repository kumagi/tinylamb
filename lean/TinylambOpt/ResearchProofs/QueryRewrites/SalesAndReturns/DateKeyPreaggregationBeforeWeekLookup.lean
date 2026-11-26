import TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns
import TinylambOpt.ResearchProofs.QueryRewrites.NullableSum

/-!
# DateKeyPreaggregationBeforeWeekLookup

A nullable sum for a selected week and weekday is unchanged when sales are first grouped
by date key and the qualifying date subtotals are added.

The aggregation can resolve the calendar attributes once per distinct date key and merge
NULL-aware states.
-/

namespace TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.DateKeyPreaggregationBeforeWeekLookup

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns.DateKeyPreaggregationBeforeWeekLookup.Goal := by
  intro sales weekDay cell
  exact (NullableSum.nsum_group_by_key (fun day => weekDay day == cell) sales).symm

end TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.DateKeyPreaggregationBeforeWeekLookup
