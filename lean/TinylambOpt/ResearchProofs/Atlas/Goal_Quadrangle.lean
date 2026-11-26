import TinylambOpt.ResearchGoals.Atlas.PlanSearch
import TinylambOpt.ResearchProofs.Atlas.PlanSearch_Quadrangle

/-!
The SQL optimizer can prune or reorganize its plan search only under the relevant
ordering and cost assumptions.

Proved

- `AlphabeticMerge.Quadrangle` — `∀ a b c, b ≠ [] → best (a++b) + best (b++c) ≤ best
  (a++b++c) + best b`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_Quadrangle

open TinylambOpt.ResearchGoals.Atlas.PlanSearch
open TinylambOpt.ResearchGoals.Atlas.PlanSearch.AlphabeticMerge

theorem sound_Quadrangle : AlphabeticMerge.Quadrangle.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.PlanSearch_Quadrangle.sound_Quadrangle

end TinylambOpt.ResearchProofs.Atlas.Goal_Quadrangle
