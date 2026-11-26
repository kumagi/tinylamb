import TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Common_CalendarRanges
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.YearsCommon
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Facts_Month

/-!
Date and timestamp predicates can use calendar bucket boundaries and range seeks where
the modeled endpoints agree.

Refuted by counterexample

- `Month.AverageDivision` — Can the optimizer replace calendar lookup by scaling with
  the mean width?
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Goal_Month_AverageDivision

open TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges
open TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges.Month
open TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges
open TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Facts_Month

theorem refute : ¬ Month.AverageDivision.Goal := by
  intro h
  have hh := h 59
  have hf : fast Month.model 59 = 2 := by native_decide
  have hd : 59 * Month.model.bins / Month.model.days = 1 := by native_decide
  omega


end TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Goal_Month_AverageDivision
