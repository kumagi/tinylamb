import TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Common_CalendarRanges
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.YearsCommon
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Facts_FiscalApril

/-!
Date and timestamp predicates can use calendar bucket boundaries and range seeks where
the modeled endpoints agree.

Proved

- `FiscalApril.LeapAhead` — `∀ y, span model y = years (y+2) - years (y+1)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Goal_FiscalApril_LeapAhead

open TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges
open TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges.FiscalApril
open TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges
open TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Facts_FiscalApril

theorem sound : FiscalApril.LeapAhead.Goal := by
  intro y
  unfold FiscalApril.model span
  change (monthStart (12*(y+1)+3) - monthStart 3) - (monthStart (12*y+3) - monthStart 3) = years (y+1+1)-years (y+1)
  have hf0 := fiscalStart y
  have hf1 := fiscalStart (y+1)
  rw [hf1, hf0]
  have hyd := years_diff_eq y
  have hyd1 := years_diff_eq (y+1)
  by_cases h0 : years (y+1) - years y == 366
  · have hd0 : years (y+1) - years y = 366 := beq_iff_eq.mp h0
    by_cases h1 : years (y+1+1) - years (y+1) == 366
    · have hd1 : years (y+1+1) - years (y+1) = 366 := beq_iff_eq.mp h1
      simp [h0, h1] at *
      omega
    · have hd1 : years (y+1+1) - years (y+1) ≠ 366 := by intro hh; exact h1 (beq_iff_eq.mpr hh)
      simp [h0, h1] at *
      omega
  · have hd0 : years (y+1) - years y ≠ 366 := by intro hh; exact h0 (beq_iff_eq.mpr hh)
    by_cases h1 : years (y+1+1) - years (y+1) == 366
    · have hd1 : years (y+1+1) - years (y+1) = 366 := beq_iff_eq.mp h1
      simp [h0, h1] at *
      omega
    · have hd1 : years (y+1+1) - years (y+1) ≠ 366 := by intro hh; exact h1 (beq_iff_eq.mpr hh)
      simp [h0, h1] at *
      omega


end TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Goal_FiscalApril_LeapAhead
