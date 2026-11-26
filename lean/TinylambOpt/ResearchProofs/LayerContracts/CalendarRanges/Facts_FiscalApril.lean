import TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Common_CalendarRanges
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.YearsCommon

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Facts_FiscalApril

open TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges
open TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges.FiscalApril
open TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges

theorem h0 : model.start 0 = 0 := by
  change monthStart 3 - monthStart 3 = 0
  omega
theorem hbins : 0 < model.bins := by native_decide
theorem hcyc : cycleShift model := by
  intro i
  change monthStart (12*(i+400)+3) - monthStart 3 = (monthStart (12*i+3) - monthStart 3) + 146097
  rw [show 12*(i+400)+3 = (12*i+3)+4800 by omega]
  rw [monthStart_add_4800]
  have hle := monthStart_mono 3 (12*i+3) (by omega)
  omega
theorem hmono : ∀ i, i < model.bins → model.start i < model.start (i+1) := by
  intro i hi
  change monthStart (12*i+3) - monthStart 3 < monthStart (12*(i+1)+3) - monthStart 3
  have hlt := monthStart_strict (12*i+3) (12*(i+1)+3) (by omega)
  have hle := monthStart_mono 3 (12*i+3) (by omega)
  omega

theorem locateP : Locate.Goal := gen_locate model h0 hcyc hbins hmono
theorem predicateRangeP : PredicateRange.Goal := gen_predicateRange model h0 hcyc hbins hmono
theorem predecessorP : Predecessor.Goal := gen_predecessor model h0 hcyc hbins hmono
theorem cyclePopulationP : CyclePopulation.Goal := gen_cyclePopulation model h0 hcyc hbins hmono
theorem coverBlocksP : CoverBlocks.Goal := gen_coverBlocks model h0 hcyc hbins hmono


theorem cycleShiftP : CycleShift.Goal := hcyc

theorem constantWidth_refute : ¬ ConstantWidth.Goal := by
  intro h
  have h4 := h 4
  have hs1 : model.start 1 = 365 := by native_decide
  have hs4 : model.start 4 = 1461 := by native_decide
  have hsp : span model 0 = 365 := by
    show model.start 1 - model.start 0 = 365
    rw [hs1, h0]
  rw [hsp, hs4] at h4
  exact absurd h4 (by decide)

theorem averageDivision_refute : ¬ AverageDivision.Goal := by
  intro h
  have h365 := h 365
  have hfast : fast model 365 = 1 := by
    have hlo : model.start 1 ≤ 365 := by simp [model, monthStart, years]
    have hhi : 365 < model.start 2 := by simp [model, monthStart, years]
    exact (predicateRangeP 365 1).mpr ⟨hlo, hhi⟩
  rw [hfast] at h365
  simp [model] at h365

private theorem e_val (z : Nat) :
    (if years (z+1) - years z == 366 then 1 else 0) = years (z+1) - years z - 365 := by
  have hd := years_diff_bounds z
  by_cases hleap : years (z+1) - years z == 366
  · rw [if_pos hleap]
    have := beq_iff_eq.mp hleap
    omega
  · rw [if_neg hleap]
    have hne : years (z+1) - years z ≠ 366 := fun hh => hleap (beq_iff_eq.mpr hh)
    omega

theorem leapAheadP : LeapAhead.Goal := by
  intro y
  change (model.start (y+1) - model.start y) = years (y+2) - years (y+1)
  show (monthStart (12*(y+1)+3) - monthStart 3) - (monthStart (12*y+3) - monthStart 3)
      = years (y+2) - years (y+1)
  rw [fiscalStart (y+1), fiscalStart y, e_val (y+1), e_val y]
  show (years (y+1) + (years (y+2) - years (y+1) - 365))
      - (years y + (years (y+1) - years y - 365)) = years (y+2) - years (y+1)
  have d1 : 365 ≤ years (y+2) - years (y+1) := (years_diff_bounds (y+1)).1
  have d0 : 365 ≤ years (y+1) - years y := (years_diff_bounds y).1
  omega

theorem aprilEdgeP : AprilEdge.Goal := by
  intro y
  have hlo : model.start y ≤ monthStart (12*y+3) - monthStart 3 := by
    simp [model]
  have hhi : monthStart (12*y+3) - monthStart 3 < model.start (y+1) := by
    change monthStart (12*y+3) - monthStart 3 < monthStart (12*(y+1)+3) - monthStart 3
    have hlt := monthStart_strict (12*y+3) (12*(y+1)+3) (by omega)
    have hle := monthStart_mono 3 (12*y+3) (by omega)
    omega
  exact (predicateRangeP (monthStart (12*y+3) - monthStart 3) y).mpr ⟨hlo, hhi⟩

end TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Facts_FiscalApril
