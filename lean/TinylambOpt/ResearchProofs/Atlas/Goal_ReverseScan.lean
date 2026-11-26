import TinylambOpt.ResearchGoals.Atlas.PlanSearch
import TinylambOpt.ResearchProofs.Atlas.PlanSearch_ReverseScan

/-!
The SQL optimizer can prune or reorganize its plan search only under the relevant
ordering and cost assumptions.

Proved

- `ParallelCuts.ReverseScan` — `∀ cap xs, (∀ w ∈ xs, w ≤ cap) → required cap xs.reverse
  = required cap xs`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_ReverseScan

open TinylambOpt.ResearchGoals.Atlas.PlanSearch
open TinylambOpt.ResearchGoals.Atlas.PlanSearch.ParallelCuts

theorem sound_ReverseScan : ParallelCuts.ReverseScan.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.PlanSearch_ReverseScan.sound_ReverseScan

end TinylambOpt.ResearchProofs.Atlas.Goal_ReverseScan
