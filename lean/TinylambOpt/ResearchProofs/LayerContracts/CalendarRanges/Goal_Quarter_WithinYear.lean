import TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Common_CalendarRanges
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.YearsCommon
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Facts_Quarter

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Goal_Quarter_WithinYear

open TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges
open TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges.Quarter
open TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges
open TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Facts_Quarter

theorem sound : Quarter.WithinYear.Goal := by
  intro y
  have hm9 := monthStart_of y 9 (by omega : 9 < 12)
  have hm9' : monthStart (12*y+9) = years y + 273 + (if years (y+1)-years y == 366 then 1 else 0) := by rw [hm9]; simp
  have hy' : years (y+1) = years y + 365 + (if years (y+1)-years y == 366 then 1 else 0) := by
    have hd := years_diff_eq y
    omega
  unfold Quarter.model
  change monthStart (3*(4*y+3)) < years (y+1)
  rw [show 3*(4*y+3)=12*y+9 by omega]
  rw [hm9']
  by_cases hleap : years (y+1)-years y == 366
  · have hd := beq_iff_eq.mp hleap
    simp [hleap]
    omega
  · have hd : years (y+1)-years y ≠ 366 := by intro hh; exact hleap (beq_iff_eq.mpr hh)
    have hb := years_diff_bounds y
    simp [hleap]
    omega


end TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Goal_Quarter_WithinYear
