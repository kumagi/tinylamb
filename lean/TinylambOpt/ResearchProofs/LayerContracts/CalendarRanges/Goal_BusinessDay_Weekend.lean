import TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Common_CalendarRanges
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.YearsCommon
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Facts_BusinessDay

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Goal_BusinessDay_Weekend

open TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges
open TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges.BusinessDay
open TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges
open TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Facts_BusinessDay

theorem sound : BusinessDay.Weekend.Goal := by
  intro w
  have hs5 : BusinessDay.model.start (5*w+4) ≤ 7*w+5 ∧ 7*w+5 < BusinessDay.model.start (5*w+4+1) := by
    unfold BusinessDay.model
    simp
    omega
  have hs6 : BusinessDay.model.start (5*w+4) ≤ 7*w+6 ∧ 7*w+6 < BusinessDay.model.start (5*w+4+1) := by
    unfold BusinessDay.model
    simp
    omega
  have h5 := predicateRangeP (7*w+5) (5*w+4) |>.mpr hs5
  have h6 := predicateRangeP (7*w+6) (5*w+4) |>.mpr hs6
  omega


end TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Goal_BusinessDay_Weekend
