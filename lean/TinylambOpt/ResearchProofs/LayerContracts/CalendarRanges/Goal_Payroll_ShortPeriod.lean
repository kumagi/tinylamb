import TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Common_CalendarRanges
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.YearsCommon
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Facts_Payroll

/-!
Date and timestamp predicates can use calendar bucket boundaries and range seeks where
the modeled endpoints agree.

Proved

- `Payroll.ShortPeriod` — `∀ n, span model (2*n) = 13`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Goal_Payroll_ShortPeriod

open TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges
open TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges.Payroll
open TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges
open TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Facts_Payroll

theorem sound : Payroll.ShortPeriod.Goal := by
  intro n
  unfold Payroll.model span
  change (2*n+1)/2*28 + 13*((2*n+1)%2) - ((2*n)/2*28 + 13*((2*n)%2)) = 13
  have h1 : (2*n+1)/2 = n := by omega
  have h2 : (2*n+1)%2 = 1 := by omega
  have h3 : (2*n)/2 = n := by omega
  have h4 : (2*n)%2 = 0 := by omega
  rw [h1, h2, h3, h4]
  omega


end TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Goal_Payroll_ShortPeriod
