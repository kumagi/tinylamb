import TinylambOpt.ResearchGoals.Atlas.PlanSearch
import TinylambOpt.ResearchProofs.Atlas.PlanSearch_ReservationSlope

/-!
The SQL optimizer can prune or reorganize its plan search only under the relevant
ordering and cost assumptions.

Proved

- `RiskEnvelope.ReservationSlope` — `∀ xs z a b, (loss xs (z+1) a b:Int) - loss xs z a
  b = (b:Int)*mass xs (· ≤ z) - (a:Int)*mass xs (· > z)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_ReservationSlope

open TinylambOpt.ResearchGoals.Atlas.PlanSearch
open TinylambOpt.ResearchGoals.Atlas.PlanSearch.RiskEnvelope

theorem sound_ReservationSlope : RiskEnvelope.ReservationSlope.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.PlanSearch_ReservationSlope.sound_ReservationSlope

end TinylambOpt.ResearchProofs.Atlas.Goal_ReservationSlope
