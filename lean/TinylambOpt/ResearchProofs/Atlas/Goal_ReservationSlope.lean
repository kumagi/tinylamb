import TinylambOpt.ResearchGoals.Atlas.PlanSearch
import TinylambOpt.ResearchProofs.Atlas.PlanSearch_ReservationSlope

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_ReservationSlope

open TinylambOpt.ResearchGoals.Atlas.PlanSearch
open TinylambOpt.ResearchGoals.Atlas.PlanSearch.RiskEnvelope

theorem sound_ReservationSlope : RiskEnvelope.ReservationSlope.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.PlanSearch_ReservationSlope.sound_ReservationSlope

end TinylambOpt.ResearchProofs.Atlas.Goal_ReservationSlope
