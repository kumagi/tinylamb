import TinylambOpt.ResearchGoals.Atlas.PlanSearch
import TinylambOpt.ResearchProofs.Atlas.PlanSearch_ReverseScan

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_ReverseScan

open TinylambOpt.ResearchGoals.Atlas.PlanSearch
open TinylambOpt.ResearchGoals.Atlas.PlanSearch.ParallelCuts

theorem sound_ReverseScan : ParallelCuts.ReverseScan.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.PlanSearch_ReverseScan.sound_ReverseScan

end TinylambOpt.ResearchProofs.Atlas.Goal_ReverseScan
