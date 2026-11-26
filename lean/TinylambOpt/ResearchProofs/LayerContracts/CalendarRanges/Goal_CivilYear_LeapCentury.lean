import TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Common_CalendarRanges
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.YearsCommon
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Facts_CivilYear

/-!
Date and timestamp predicates can use calendar bucket boundaries and range seeks where
the modeled endpoints agree.

Proved

- `CivilYear.LeapCentury` — `∀ c, span model (400*(c+1)-1) = 366`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Goal_CivilYear_LeapCentury

open TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges
open TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges.CivilYear
open TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges
open TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Facts_CivilYear

theorem sound : CivilYear.LeapCentury.Goal := by
  intro c
  unfold CivilYear.model span
  change years (400*(c+1)) - years (400*(c+1)-1) = 366
  have hidx : 400*(c+1)-1 = 400*c+399 := by omega
  rw [hidx]
  unfold years
  omega


end TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Goal_CivilYear_LeapCentury
