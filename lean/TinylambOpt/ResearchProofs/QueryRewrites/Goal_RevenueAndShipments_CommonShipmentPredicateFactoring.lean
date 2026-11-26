import TinylambOpt.ResearchGoals.QueryRewrites.RevenueAndShipments

/-!
# CommonShipmentPredicateFactoring

A shared shipment condition can be pulled out of a disjunction of item and quantity
predicates without changing which modeled rows pass.

The filter can evaluate the air-shipment condition once and use each item's quantity
range in the remaining branches.
-/

set_option linter.unusedSimpArgs false
set_option linter.deprecated false
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.QueryRewrites.RevenueAndShipments.Goal_CommonShipmentPredicateFactoring
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites.RevenueAndShipments
open TinylambOpt.ResearchGoals.QueryRewrites


def bands : List (Nat × Nat) := [(5,15),(15,25),(29,39)]

theorem accepts_iff (air : Bool) (tag q : Nat) :
    (air && (bands[tag]?).any fun (l, h) => l ≤ q && q ≤ h) =
    (air && ((tag == 0 && 5 ≤ q && q ≤ 15) ||
      (tag == 1 && 15 ≤ q && q ≤ 25) || (tag == 2 && 29 ≤ q && q ≤ 39))) := by
  cases air <;> simp [bands]
  cases tag with
  | zero => simp
  | succ t =>
      cases t with
      | zero => simp
      | succ u =>
          cases u with
          | zero => simp
          | succ v =>
              have hget : bands[v + 3]? = none := by
                apply List.getElem?_eq_none
                simp [bands]
              simpa [Nat.add_assoc] using hget


theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.RevenueAndShipments.CommonShipmentPredicateFactoring.Goal := by
  intro rows
  dsimp
  apply List.filter_congr
  intro r hr
  rcases r with ⟨air, tag, q⟩
  simpa [TinylambOpt.ResearchProofs.QueryRewrites.RevenueAndShipments.Goal_CommonShipmentPredicateFactoring.bands] using TinylambOpt.ResearchProofs.QueryRewrites.RevenueAndShipments.Goal_CommonShipmentPredicateFactoring.accepts_iff air tag q
end TinylambOpt.ResearchProofs.QueryRewrites.RevenueAndShipments.Goal_CommonShipmentPredicateFactoring
