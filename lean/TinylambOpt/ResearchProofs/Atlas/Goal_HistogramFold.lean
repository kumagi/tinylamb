import TinylambOpt.ResearchGoals.Atlas.PlanSearch
import TinylambOpt.ResearchProofs.Atlas.PlanSearch_HistogramFold

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_HistogramFold

open TinylambOpt.ResearchGoals.Atlas.PlanSearch
open TinylambOpt.ResearchGoals.Atlas.PlanSearch.RiskEnvelope

theorem sound_HistogramFold : RiskEnvelope.HistogramFold.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.PlanSearch_HistogramFold.sound_HistogramFold

end TinylambOpt.ResearchProofs.Atlas.Goal_HistogramFold
