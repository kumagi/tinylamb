import TinylambOpt.ResearchGoals.Atlas.PlanSearch
import TinylambOpt.ResearchProofs.Atlas.PlanSearch_MonotoneCut

/-!
The SQL optimizer can prune or reorganize its plan search only under the relevant
ordering and cost assumptions.

Proved

- `AlphabeticMerge.MonotoneCut` — `∀ ws v, 2 ≤ ws.length → cut ws ≤ cut (ws++[v])`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_MonotoneCut

open TinylambOpt.ResearchGoals.Atlas.PlanSearch
open TinylambOpt.ResearchGoals.Atlas.PlanSearch.AlphabeticMerge

theorem sound_MonotoneCut : AlphabeticMerge.MonotoneCut.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.PlanSearch_MonotoneCut.sound_MonotoneCut

end TinylambOpt.ResearchProofs.Atlas.Goal_MonotoneCut
