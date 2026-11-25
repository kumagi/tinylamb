import TinylambOpt.ResearchGoals.Atlas.PlanSearch

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_RiskEnvelope_MeanReserve

open TinylambOpt.ResearchGoals.Atlas.PlanSearch.RiskEnvelope

/-- The claim that the (mass-weighted) mean reservation minimizes the absolute
    loss is false.  Counterexample from `issues/research/atlas.md`: loads
    `(0,3)` and `(8,1)` give mass 4 and mean 2, but reserving 2 costs 12 while
    reserving 0 costs only 8. -/
theorem disproof_RiskEnvelope_MeanReserve : ¬ MeanReserve.Goal := by
  intro h
  let xs : Costs := [(0, 3), (8, 1)]
  have hmass : 0 < mass xs (fun _ => true) := by
    native_decide
  let avg : Int := (xs.map (fun (v, w) => v * (w : Int))).sum / (mass xs (fun _ => true) : Int)
  have hbad := h xs hmass 0
  have hnot : ¬ loss xs avg 1 1 ≤ loss xs 0 1 1 := by
    native_decide
  exact hnot hbad

end TinylambOpt.ResearchProofs.Atlas.Goal_RiskEnvelope_MeanReserve
