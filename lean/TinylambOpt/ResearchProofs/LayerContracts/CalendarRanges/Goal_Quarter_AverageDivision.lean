import TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Common_CalendarRanges
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.YearsCommon
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Facts_Quarter

/-!
Date and timestamp predicates can use calendar bucket boundaries and range seeks where
the modeled endpoints agree.

Refuted by counterexample

- `Quarter.AverageDivision` — Can the optimizer replace calendar lookup by scaling with
  the mean width?
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Goal_Quarter_AverageDivision

open TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges
open TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges.Quarter
open TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges
open TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Facts_Quarter

theorem refute : ¬ Quarter.AverageDivision.Goal := by
  intro h
  have hh := h 90
  have hf : fast Quarter.model 90 = 1 := by native_decide
  have hd : 90 * Quarter.model.bins / Quarter.model.days = 0 := by native_decide
  omega


end TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Goal_Quarter_AverageDivision
