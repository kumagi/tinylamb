import TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Common_CalendarRanges
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.YearsCommon
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Facts_IsoWeekYear

/-!
Date and timestamp predicates can use calendar bucket boundaries and range seeks where
the modeled endpoints agree.

Refuted by counterexample

- `IsoWeekYear.ConstantWidth` — Can the JIT use a constant width learned from the first
  bucket?
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Goal_IsoWeekYear_ConstantWidth

open TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges
open TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges.IsoWeekYear
open TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges
open TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Facts_IsoWeekYear

theorem refute : ¬ IsoWeekYear.ConstantWidth.Goal := by
  intro h
  have hh := h 4
  have hs : span IsoWeekYear.model 0 = 364 := by native_decide
  have hv : IsoWeekYear.model.start 4 = 1463 := by native_decide
  omega


end TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Goal_IsoWeekYear_ConstantWidth
