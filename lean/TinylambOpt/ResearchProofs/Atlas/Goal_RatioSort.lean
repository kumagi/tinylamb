import TinylambOpt.ResearchGoals.Atlas.PlanSearch
import TinylambOpt.ResearchProofs.Atlas.PlanSearch_RatioSort

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_RatioSort

open TinylambOpt.ResearchGoals.Atlas.PlanSearch
open TinylambOpt.ResearchGoals.Atlas.PlanSearch.PredicateOrder

theorem sound_RatioSort : PredicateOrder.RatioSort.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.PlanSearch_RatioSort.sound_RatioSort

end TinylambOpt.ResearchProofs.Atlas.Goal_RatioSort
