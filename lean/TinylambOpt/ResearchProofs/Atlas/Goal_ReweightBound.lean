import TinylambOpt.ResearchGoals.Atlas.PlanSearch
import TinylambOpt.ResearchProofs.Atlas.PlanSearch_ReweightBound

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_ReweightBound

open TinylambOpt.ResearchGoals.Atlas.PlanSearch
open TinylambOpt.ResearchGoals.Atlas.PlanSearch.AlphabeticMerge

theorem sound_ReweightBound : AlphabeticMerge.ReweightBound.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.PlanSearch_ReweightBound.sound_ReweightBound

end TinylambOpt.ResearchProofs.Atlas.Goal_ReweightBound
