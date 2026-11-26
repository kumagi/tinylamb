import TinylambOpt.ResearchGoals.QueryRewrites.AnalyticalJoins
import TinylambOpt.ResearchProofs.QueryRewrites.NullableSum

/-!
# PhysicalKeySubtotalRollup

Summing the subtotals of physical keys with the selected dimension labels preserves the
nullable sum of the corresponding sales rows.

The executor can aggregate by product and store IDs before looking up category and
region for the rollup.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false
set_option linter.deprecated false


namespace TinylambOpt.ResearchProofs.QueryRewrites.AnalyticalJoins.PhysicalKeySubtotalRollup
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchProofs.QueryRewrites.NullableSum (nsum_group_by_key)

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.AnalyticalJoins.PhysicalKeySubtotalRollup.Goal := by
  intro sales labels k
  dsimp only
  exact nsum_group_by_key (fun p => labels p == k) sales


end TinylambOpt.ResearchProofs.QueryRewrites.AnalyticalJoins.PhysicalKeySubtotalRollup
