import TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Common_CalendarRanges
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.YearsCommon
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Facts_FiscalApril

/-!
Date and timestamp predicates can use calendar bucket boundaries and range seeks where
the modeled endpoints agree.

Proved

- `FiscalApril.AprilEdge` — `∀ y, fast model (monthStart (12*y+3)-monthStart 3) = y`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Goal_FiscalApril_AprilEdge

open TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges
open TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges.FiscalApril
open TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges
open TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Facts_FiscalApril

theorem sound : FiscalApril.AprilEdge.Goal := by
  intro y
  have hstrict := start_strict_mono FiscalApril.model h0 hcyc hbins hmono y
  have hs : FiscalApril.model.start y ≤ FiscalApril.model.start y ∧
      FiscalApril.model.start y < FiscalApril.model.start (y+1) := by
    exact ⟨by omega, hstrict⟩
  exact predicateRangeP (FiscalApril.model.start y) y |>.mpr hs


end TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Goal_FiscalApril_AprilEdge
