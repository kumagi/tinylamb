import TinylambOpt.ResearchGoals.Atlas.PlanSearch
import TinylambOpt.ResearchProofs.Atlas.PlanSearch_QuantileReserve

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_RiskEnvelope_QuantileReserve

open TinylambOpt.ResearchGoals.Atlas.PlanSearch
open TinylambOpt.ResearchGoals.Atlas.PlanSearch.RiskEnvelope

/-!
  Small-input sanity checks for the weighted-quantile claim.  The two
  inequalities choose `z` as a weighted median, and then `z` minimises the
  asymmetric absolute-loss objective.  A tiny exhaustive check over a few
  candidate reserves confirms the statement before using the general proof.
-/

private def sampleCosts : Costs := [(0, 1), (1, 2), (3, 1)]

private def candidates : List Int := [-2, -1, 0, 1, 2, 3, 4]

#eval decide ((1 + 1) * mass sampleCosts (· < 1) ≤
    1 * mass sampleCosts (fun _ => true))
#eval decide (1 * mass sampleCosts (fun _ => true) ≤
    (1 + 1) * mass sampleCosts (· ≤ 1))
#eval decide (candidates.all (fun y => loss sampleCosts 1 1 1 ≤ loss sampleCosts y 1 1))

theorem sound_RiskEnvelope_QuantileReserve : RiskEnvelope.QuantileReserve.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.PlanSearch_QuantileReserve.sound_QuantileReserve

end TinylambOpt.ResearchProofs.Atlas.Goal_RiskEnvelope_QuantileReserve
