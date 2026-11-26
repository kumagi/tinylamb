import TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Common_CalendarRanges
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.YearsCommon
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Facts_HalfMonth

/-!
Date and timestamp predicates can use calendar bucket boundaries and range seeks where
the modeled endpoints agree.

Proved

- `HalfMonth.LateHalf` — `∀ m, span model (2*m+1) = monthStart (m+1)-monthStart m-15`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Goal_HalfMonth_LateHalf

open TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges
open TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges.HalfMonth
open TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges
open TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Facts_HalfMonth

theorem sound : HalfMonth.LateHalf.Goal := by
  intro m
  unfold HalfMonth.model span
  change (monthStart ((2*m+2)/2) + 15*((2*m+2)%2)) - (monthStart ((2*m+1)/2) + 15*((2*m+1)%2)) =
    monthStart (m+1) - monthStart m - 15
  have h1 : (2*m+2)/2 = m+1 := by omega
  have h2 : (2*m+2)%2 = 0 := by omega
  have h3 : (2*m+1)/2 = m := by omega
  have h4 : (2*m+1)%2 = 1 := by omega
  rw [h1, h2, h3, h4]
  have hg := monthStart_gap m
  omega


end TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Goal_HalfMonth_LateHalf
