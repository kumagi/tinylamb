import TinylambOpt.ResearchGoals.Atlas.PlanSearch

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.PlanSearch_MeanReserve

open TinylambOpt.ResearchGoals.Atlas.PlanSearch
open TinylambOpt.ResearchGoals.Atlas.PlanSearch.RiskEnvelope

-- Counterexample: xs = [(0,3),(8,1)] has mass 4 and weighted average
-- (0*3 + 8*1)/4 = 2.  The absolute loss at 2 is 12, but at 0 it is 8,
-- so the (integer-)mean reservation is not a minimiser.
theorem disproof_MeanReserve : ¬ RiskEnvelope.MeanReserve.Goal := by
  intro h
  let xs : Costs := [(0, 3), (8, 1)]
  have hmass : 0 < mass xs (fun _ => true) := by
    native_decide
  have hh := h xs hmass 0
  have hneq : ¬ (loss xs
        ((xs.map (fun (v, w) => v * (w : Int))).sum /
          (mass xs (fun _ => true) : Int)) 1 1 ≤ loss xs 0 1 1) := by
    native_decide
  exact hneq hh

end TinylambOpt.ResearchProofs.Atlas.PlanSearch_MeanReserve
