import TinylambOpt.ResearchGoals.QueryRewrites.RevenueAndShipments

/-!
# TopCustomersBeforeWideAttributeLookup

The same ordered top customers are selected when the ranking uses the narrow keys and
scores and their attributes are attached afterward.

The executor can defer reading wide customer attributes until the winning rows are
known, using the same ranking and tie order.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.QueryRewrites.RevenueAndShipments.TopCustomersBeforeWideAttributeLookup
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites.RevenueAndShipments

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.RevenueAndShipments.TopCustomersBeforeWideAttributeLookup.Goal := by
  intro ids rank attrs
  dsimp [sortedBy]
  rw [← List.map_mergeSort (f := fun i => (i, attrs i))
        (r := fun a b : Nat => decide (rank a ≤ rank b))
        (s := fun a b : Nat × String => decide (rank a.1 ≤ rank b.1))]
  · rw [List.map_take]
  · intro a ha b hb
    rfl
end TinylambOpt.ResearchProofs.QueryRewrites.RevenueAndShipments.TopCustomersBeforeWideAttributeLookup
