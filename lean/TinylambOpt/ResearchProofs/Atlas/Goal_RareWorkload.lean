import TinylambOpt.ResearchGoals.Atlas.PlanSearch
import TinylambOpt.ResearchProofs.Atlas.PlanSearch_RareWorkload

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_RareWorkload

open TinylambOpt.ResearchGoals.Atlas.PlanSearch
open TinylambOpt.ResearchGoals.Atlas.PlanSearch.RiskEnvelope

theorem sound_RareWorkload : RiskEnvelope.RareWorkload.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.PlanSearch_RareWorkload.sound_RareWorkload

end TinylambOpt.ResearchProofs.Atlas.Goal_RareWorkload
