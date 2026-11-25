import TinylambOpt.ResearchGoals.Atlas.PlanSearch
import TinylambOpt.ResearchProofs.Atlas.PlanSearch_OptimalTree

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_OptimalTree

open TinylambOpt.ResearchGoals.Atlas.PlanSearch
open TinylambOpt.ResearchGoals.Atlas.PlanSearch.AlphabeticMerge

theorem sound_OptimalTree : AlphabeticMerge.OptimalTree.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.PlanSearch_OptimalTree.sound_OptimalTree

end TinylambOpt.ResearchProofs.Atlas.Goal_OptimalTree
