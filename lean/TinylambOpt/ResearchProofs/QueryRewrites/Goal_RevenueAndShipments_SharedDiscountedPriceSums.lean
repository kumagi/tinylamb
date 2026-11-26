import TinylambOpt.ResearchGoals.QueryRewrites.RevenueAndShipments

/-!
# SharedDiscountedPriceSums

For date-qualified rows, computing the discounted price once preserves both sums that
use it. The scan can share the multiplication across aggregate updates.
-/


set_option linter.unusedSimpArgs false
set_option linter.deprecated false
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.QueryRewrites.RevenueAndShipments.Goal_SharedDiscountedPriceSums

open TinylambOpt.ResearchGoals.QueryRewrites

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.RevenueAndShipments.SharedDiscountedPriceSums.Goal := by
  intro rows
  dsimp only
  apply Prod.ext <;> simp [total, List.map_map, Function.comp_def]

end TinylambOpt.ResearchProofs.QueryRewrites.RevenueAndShipments.Goal_SharedDiscountedPriceSums

