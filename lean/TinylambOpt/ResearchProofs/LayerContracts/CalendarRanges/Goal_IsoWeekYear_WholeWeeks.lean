import TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Common_CalendarRanges
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.YearsCommon
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Facts_IsoWeekYear

/-!
Date and timestamp predicates can use calendar bucket boundaries and range seeks where
the modeled endpoints agree.

Proved

- `IsoWeekYear.WholeWeeks` — `∀ i, span model i = 364 ∨ span model i = 371`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Goal_IsoWeekYear_WholeWeeks

open TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges
open TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges.IsoWeekYear
open TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges
open TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Facts_IsoWeekYear

theorem sound : IsoWeekYear.WholeWeeks.Goal := by
  intro i
  unfold IsoWeekYear.model span
  change ((years (i+1)+3)/7*7 - (years i+3)/7*7 = 364 ∨
          (years (i+1)+3)/7*7 - (years i+3)/7*7 = 371)
  have hd := years_diff_bounds i
  omega


end TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Goal_IsoWeekYear_WholeWeeks
