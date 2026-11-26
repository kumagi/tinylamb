import TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Common_CalendarRanges
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.YearsCommon
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Facts_HalfMonth

/-!
Date and timestamp predicates can use calendar bucket boundaries and range seeks where
the modeled endpoints agree.

Refuted by counterexample

- `HalfMonth.ConstantWidth` — Can the JIT use a constant width learned from the first
  bucket?
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Goal_HalfMonth_ConstantWidth

open TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges
open TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges.HalfMonth
open TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges
open TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Facts_HalfMonth

theorem refute : ¬ HalfMonth.ConstantWidth.Goal := by
  intro h
  have hh := h 2
  have hs : span HalfMonth.model 0 = 15 := by native_decide
  have hv : HalfMonth.model.start 2 = 31 := by native_decide
  omega


end TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Goal_HalfMonth_ConstantWidth
