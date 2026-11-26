import TinylambOpt.ResearchGoals.Atlas.PlanSearch
import TinylambOpt.ResearchProofs.Atlas.PlanSearch_ReweightBound

/-!
The SQL optimizer can prune or reorganize its plan search only under the relevant
ordering and cost assumptions.

Proved

- `AlphabeticMerge.ReweightBound` — `∀ t i d, i < (weights t).length → work t = best
  (weights t) → best ((weights t).modify i (·+d)) ≤ work t + d*depth t i`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_ReweightBound

open TinylambOpt.ResearchGoals.Atlas.PlanSearch
open TinylambOpt.ResearchGoals.Atlas.PlanSearch.AlphabeticMerge

theorem sound_ReweightBound : AlphabeticMerge.ReweightBound.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.PlanSearch_ReweightBound.sound_ReweightBound

end TinylambOpt.ResearchProofs.Atlas.Goal_ReweightBound
