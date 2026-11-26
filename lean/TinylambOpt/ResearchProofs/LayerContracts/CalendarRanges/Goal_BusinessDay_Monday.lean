import TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Common_CalendarRanges
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.YearsCommon
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Facts_BusinessDay

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Goal_BusinessDay_Monday

open TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges
open TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges.BusinessDay
open TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges
open TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Facts_BusinessDay

theorem sound : BusinessDay.Monday.Goal := by
  intro w
  have hs : BusinessDay.model.start (5*w) ≤ 7*w ∧ 7*w < BusinessDay.model.start (5*w+1) := by
    unfold BusinessDay.model
    simp
    omega
  exact predicateRangeP (7*w) (5*w) |>.mpr hs


end TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Goal_BusinessDay_Monday
