import TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns
import TinylambOpt.ResearchProofs.QueryRewrites.NullableSum

/-!
# HavingFilteredNullableRollup

The nullable total of qualifying detail groups agrees with the sum over their combined
input values.

The rollup can consume the subtotal of each group that passes HAVING, with NULL
preserved when no qualifying value contributes.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false


namespace TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.HavingFilteredNullableRollup
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns
open TinylambOpt.ResearchProofs.QueryRewrites.NullableSum (nsum_map_nsum)

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns.HavingFilteredNullableRollup.Goal := by
  intro cells threshold kept
  exact nsum_map_nsum kept
end TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.HavingFilteredNullableRollup
