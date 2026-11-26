import TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Common_CalendarRanges
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.YearsCommon
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Facts_HalfMonth

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Goal_HalfMonth_EarlyHalf

open TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges
open TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges.HalfMonth
open TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges
open TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Facts_HalfMonth

theorem sound : HalfMonth.EarlyHalf.Goal := by
  intro m
  unfold HalfMonth.model span
  change (monthStart ((2*m+1)/2) + 15*((2*m+1)%2)) - (monthStart ((2*m)/2) + 15*((2*m)%2)) = 15
  have h1 : (2*m)/2 = m := by omega
  have h2 : (2*m)%2 = 0 := by omega
  have h3 : (2*m+1)/2 = m := by omega
  have h4 : (2*m+1)%2 = 1 := by omega
  rw [h1, h2, h3, h4]
  omega


end TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Goal_HalfMonth_EarlyHalf
