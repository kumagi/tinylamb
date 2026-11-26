import TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Common_CalendarRanges
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.YearsCommon
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Facts_HalfMonth

/-!
Date and timestamp predicates can use calendar bucket boundaries and range seeks where
the modeled endpoints agree.

Refuted by counterexample

- `HalfMonth.AverageDivision` — Can the optimizer replace calendar lookup by scaling
  with the mean width?
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Goal_HalfMonth_AverageDivision

open TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges
open TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges.HalfMonth
open TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges
open TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Facts_HalfMonth

theorem refute : ¬ HalfMonth.AverageDivision.Goal := by
  intro h
  have hh := h 15
  have hf : fast HalfMonth.model 15 = 1 := by native_decide
  have hd : 15 * HalfMonth.model.bins / HalfMonth.model.days = 0 := by native_decide
  omega


end TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Goal_HalfMonth_AverageDivision
