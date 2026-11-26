import TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Common_CalendarRanges
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.YearsCommon
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Facts_Quarter

/-!
Date and timestamp predicates can use calendar bucket boundaries and range seeks where
the modeled endpoints agree.

Proved

- `Quarter.FirstQuarter` — `∀ y, span model (4*y) = years (y+1)-years y-275`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Goal_Quarter_FirstQuarter

open TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges
open TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges.Quarter
open TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges
open TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Facts_Quarter

theorem sound : Quarter.FirstQuarter.Goal := by
  intro y
  have hm0 := monthStart_of y 0 (by omega : 0 < 12)
  have hm3 := monthStart_of y 3 (by omega : 3 < 12)
  have hm0' : monthStart (12*y+0) = years y := by rw [hm0]; simp
  have hm3' : monthStart (12*y+3) = years y + 90 + (if years (y+1)-years y == 366 then 1 else 0) := by rw [hm3]; simp
  unfold Quarter.model span
  change monthStart (3*(4*y+1)) - monthStart (3*(4*y)) = years (y+1)-years y-275
  rw [show 3*(4*y+1)=12*y+3 by omega, show 3*(4*y)=12*y+0 by omega]
  rw [hm0', hm3']
  rw [years_diff_eq y]
  by_cases hleap : years (y+1)-years y == 366 <;> simp [hleap] <;> omega


end TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Goal_Quarter_FirstQuarter
