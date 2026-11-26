import TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Common_CalendarRanges
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.YearsCommon

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Facts_IsoWeekYear

open TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges
open TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges.IsoWeekYear
open TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges

theorem h0 : model.start 0 = 0 := by
  change (years 0 + 3)/7*7 = 0
  unfold years
  simp
theorem hbins : 0 < model.bins := by native_decide
theorem hcyc : cycleShift model := by
  intro i
  change (years (i+400)+3)/7*7 = (years i+3)/7*7 + 146097
  rw [years400 i 1]
  have hc : 146097 = 7*20871 := by omega
  rw [hc]
  omega
theorem hmono : ∀ i, i < model.bins → model.start i < model.start (i+1) := by
  intro i hi
  change (years (i+1)+3)/7*7 > (years i+3)/7*7
  have hd := years_diff_bounds i
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
  simp [model, span, years] at h4

theorem averageDivision_refute : ¬ AverageDivision.Goal := by
  intro h
  have h364 := h 364
  have hfast : fast model 364 = 1 := by
    have hlo : model.start 1 ≤ 364 := by simp [model, years]
    have hhi : 364 < model.start 2 := by simp [model, years]
    exact (predicateRangeP 364 1).mpr ⟨hlo, hhi⟩
  rw [hfast] at h364
  simp [model] at h364

theorem wholeWeeksP : WholeWeeks.Goal := by
  intro i
  unfold span model
  simp
  have hd := years_diff_bounds i
  omega

theorem januaryFourthP : JanuaryFourth.Goal := by
  intro y
  have hlo : model.start y ≤ years y + 3 := by
    change (years y + 3) / 7 * 7 ≤ years y + 3
    exact Nat.div_mul_le_self _ _
  have hhi : years y + 3 < model.start (y+1) := by
    change years y + 3 < (years (y+1) + 3) / 7 * 7
    have hd := years_diff_bounds y
    omega
  exact (predicateRangeP (years y + 3) y).mpr ⟨hlo, hhi⟩

end TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Facts_IsoWeekYear
