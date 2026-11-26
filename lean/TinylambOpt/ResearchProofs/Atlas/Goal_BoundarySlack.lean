import TinylambOpt.ResearchGoals.Atlas.PlanSearch
import TinylambOpt.ResearchProofs.Atlas.PlanSearch_BoundarySlack

/-!
The SQL optimizer can prune or reorganize its plan search only under the relevant
ordering and cost assumptions.

Proved

- `ParallelCuts.BoundarySlack` — `∀ cap a b, (∀ w ∈ a++b, w ≤ cap) → required cap a +
  required cap b - 1 ≤ required cap (a++b) ∧ required cap (a++b) ≤ required cap a +
  required cap b`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_BoundarySlack

open TinylambOpt.ResearchGoals.Atlas.PlanSearch
open TinylambOpt.ResearchGoals.Atlas.PlanSearch.ParallelCuts

theorem sound_BoundarySlack : ParallelCuts.BoundarySlack.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.PlanSearch_BoundarySlack.sound_BoundarySlack

end TinylambOpt.ResearchProofs.Atlas.Goal_BoundarySlack
