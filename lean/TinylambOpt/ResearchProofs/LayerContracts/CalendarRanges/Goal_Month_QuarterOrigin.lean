import TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Common_CalendarRanges
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.YearsCommon
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Facts_Month

/-!
Date and timestamp predicates can use calendar bucket boundaries and range seeks where
the modeled endpoints agree.

Proved

- `Month.QuarterOrigin` — `∀ d q, fast model d / 3 = q ↔ monthStart (3*q) ≤ d ∧ d <
  monthStart (3*q+3)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Goal_Month_QuarterOrigin

open TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges
open TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges.Month
open TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges
open TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Facts_Month

theorem sound : Month.QuarterOrigin.Goal := by
  intro d q
  exact gen_thirdBucket Month.model h0 hcyc hbins hmono q d


end TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Goal_Month_QuarterOrigin
