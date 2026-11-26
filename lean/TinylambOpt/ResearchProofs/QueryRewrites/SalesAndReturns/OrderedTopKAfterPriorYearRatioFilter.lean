import TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

/-!
# OrderedTopKAfterPriorYearRatioFilter

The first qualifying rows in an already ordered stream determine the requested prefix of
the filtered result.

An ordered execution path can stop evaluating ratios once it has found the required
number of rows.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000


namespace TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.OrderedTopKAfterPriorYearRatioFilter
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns.OrderedTopKAfterPriorYearRatioFilter.Goal := by
  intro ordered pass cut h
  have hlen : 100 ≤ ((ordered.take cut).filter pass).length := by
    simpa [countWhere] using h
  conv =>
    rhs
    rw [← List.take_append_drop cut ordered]
  rw [List.filter_append, List.take_append_of_le_length hlen]
end TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.OrderedTopKAfterPriorYearRatioFilter
