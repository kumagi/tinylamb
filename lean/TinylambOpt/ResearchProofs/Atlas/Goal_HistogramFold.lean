import TinylambOpt.ResearchGoals.Atlas.PlanSearch
import TinylambOpt.ResearchProofs.Atlas.PlanSearch_HistogramFold

/-!
The SQL optimizer can prune or reorganize its plan search only under the relevant
ordering and cost assumptions.

Proved

- `RiskEnvelope.HistogramFold` — `∀ xs z a b, loss xs z a b = loss ((xs.map
  Prod.fst).eraseDups.map (fun v => (v,mass xs (· == v)))) z a b`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_HistogramFold

open TinylambOpt.ResearchGoals.Atlas.PlanSearch
open TinylambOpt.ResearchGoals.Atlas.PlanSearch.RiskEnvelope

theorem sound_HistogramFold : RiskEnvelope.HistogramFold.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.PlanSearch_HistogramFold.sound_HistogramFold

end TinylambOpt.ResearchProofs.Atlas.Goal_HistogramFold
