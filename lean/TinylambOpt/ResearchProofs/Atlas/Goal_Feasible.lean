import TinylambOpt.ResearchGoals.Atlas.PlanSearch
import TinylambOpt.ResearchProofs.Atlas.PlanSearch_Feasible

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_Feasible

open TinylambOpt.ResearchGoals.Atlas.PlanSearch
open TinylambOpt.ResearchGoals.Atlas.PlanSearch.ParallelCuts

theorem sound_Feasible : ParallelCuts.Feasible.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.PlanSearch_Feasible.sound_Feasible

end TinylambOpt.ResearchProofs.Atlas.Goal_Feasible
