import TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Common_CalendarRanges
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.YearsCommon
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Facts_Retail445

/-!
Date and timestamp predicates can use calendar bucket boundaries and range seeks where
the modeled endpoints agree.

Proved

- `Retail445.FifthWeek` — `∀ q, span model (3*q+2) = 35`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Goal_Retail445_FifthWeek

open TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges
open TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges.Retail445
open TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges
open TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Facts_Retail445

theorem sound : Retail445.FifthWeek.Goal := by
  intro q
  unfold Retail445.model span
  change (3*q+2+1)/3*91 + 28*((3*q+2+1)%3) - ((3*q+2)/3*91 + 28*((3*q+2)%3)) = 35
  have h1 : (3*q+2)/3 = q := by omega
  have h2 : (3*q+2)%3 = 2 := by omega
  have h3 : (3*q+2+1)/3 = q+1 := by omega
  have h4 : (3*q+2+1)%3 = 0 := by omega
  rw [h1, h2, h3, h4]
  omega


end TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Goal_Retail445_FifthWeek
