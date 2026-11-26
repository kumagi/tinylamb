import TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Common_CalendarRanges
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.YearsCommon
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Facts_Month

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Goal_Month_February

open TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges
open TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges.Month
open TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges
open TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Facts_Month

theorem sound : Month.February.Goal := by
  intro y
  have hm1 := monthStart_of y 1 (by omega : 1 < 12)
  have hm2 := monthStart_of y 2 (by omega : 2 < 12)
  have hm1' : monthStart (12*y+1) = years y + 31 := by rw [hm1]; simp
  have hm2' : monthStart (12*y+2) = years y + 59 + (if years (y+1)-years y == 366 then 1 else 0) := by rw [hm2]; simp
  unfold Month.model span
  change monthStart (12*y+1+1) - monthStart (12*y+1) = years (y+1)-years y-337
  rw [show 12*y+1+1 = 12*y+2 by omega]
  rw [hm1', hm2']
  rw [years_diff_eq y]
  by_cases hleap : years (y+1)-years y == 366 <;> simp [hleap] <;> omega


end TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Goal_Month_February
