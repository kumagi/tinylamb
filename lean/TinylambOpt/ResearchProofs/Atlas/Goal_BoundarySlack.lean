import TinylambOpt.ResearchGoals.Atlas.PlanSearch
import TinylambOpt.ResearchProofs.Atlas.PlanSearch_BoundarySlack

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_BoundarySlack

open TinylambOpt.ResearchGoals.Atlas.PlanSearch
open TinylambOpt.ResearchGoals.Atlas.PlanSearch.ParallelCuts

theorem sound_BoundarySlack : ParallelCuts.BoundarySlack.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.PlanSearch_BoundarySlack.sound_BoundarySlack

end TinylambOpt.ResearchProofs.Atlas.Goal_BoundarySlack
