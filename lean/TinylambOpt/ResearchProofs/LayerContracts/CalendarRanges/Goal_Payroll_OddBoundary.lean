import TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Common_CalendarRanges
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.YearsCommon
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Facts_Payroll

/-!
Date and timestamp predicates can use calendar bucket boundaries and range seeks where
the modeled endpoints agree.

Proved

- `Payroll.OddBoundary` — `∀ n, fast model (28*n+13) = 2*n+1`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Goal_Payroll_OddBoundary

open TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges
open TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges.Payroll
open TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges
open TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Facts_Payroll

theorem sound : Payroll.OddBoundary.Goal := by
  intro n
  have hs : Payroll.model.start (2*n+1) ≤ 28*n+13 ∧ 28*n+13 < Payroll.model.start (2*n+1+1) := by
    unfold Payroll.model
    simp
    omega
  exact predicateRangeP (28*n+13) (2*n+1) |>.mpr hs


end TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Goal_Payroll_OddBoundary
