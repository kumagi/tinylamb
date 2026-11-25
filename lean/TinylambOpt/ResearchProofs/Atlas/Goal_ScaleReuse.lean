import TinylambOpt.ResearchGoals.Atlas.PlanSearch
import TinylambOpt.ResearchProofs.Atlas.PlanSearch_ScaleReuse

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_ScaleReuse

open TinylambOpt.ResearchGoals.Atlas.PlanSearch
open TinylambOpt.ResearchGoals.Atlas.PlanSearch.AlphabeticMerge

theorem sound_ScaleReuse : AlphabeticMerge.ScaleReuse.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.PlanSearch_ScaleReuse.sound_ScaleReuse

end TinylambOpt.ResearchProofs.Atlas.Goal_ScaleReuse
