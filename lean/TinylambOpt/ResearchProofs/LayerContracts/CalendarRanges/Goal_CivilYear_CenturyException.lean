import TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Common_CalendarRanges
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.YearsCommon
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Facts_CivilYear

/-!
Date and timestamp predicates can use calendar bucket boundaries and range seeks where
the modeled endpoints agree.

Proved

- `CivilYear.CenturyException` — `∀ c, (c+1)%4 ≠ 0 → span model (100*(c+1)-1) = 365`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Goal_CivilYear_CenturyException

open TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges
open TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges.CivilYear
open TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges
open TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Facts_CivilYear

theorem sound : CivilYear.CenturyException.Goal := by
  intro c hc
  unfold CivilYear.model span
  change years (100*(c+1)) - years (100*(c+1)-1) = 365
  have hidx : 100*(c+1)-1 = 100*c+99 := by omega
  rw [hidx]
  unfold years
  omega


end TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Goal_CivilYear_CenturyException
