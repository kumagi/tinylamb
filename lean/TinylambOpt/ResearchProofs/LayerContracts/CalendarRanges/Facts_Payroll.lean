import TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Common_CalendarRanges
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.YearsCommon

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Facts_Payroll

open TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges
open TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges.Payroll
open TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges

theorem h0 : model.start 0 = 0 := by native_decide
theorem hbins : 0 < model.bins := by native_decide
theorem hcyc : cycleShift model := by
  intro i
  unfold model
  simp
  omega
theorem hmono : ∀ i, i < model.bins → model.start i < model.start (i+1) := by
  intro i hi
  unfold model
  simp
  omega

theorem locateP : Locate.Goal := gen_locate model h0 hcyc hbins hmono
theorem predicateRangeP : PredicateRange.Goal := gen_predicateRange model h0 hcyc hbins hmono
theorem predecessorP : Predecessor.Goal := gen_predecessor model h0 hcyc hbins hmono
theorem cyclePopulationP : CyclePopulation.Goal := gen_cyclePopulation model h0 hcyc hbins hmono
theorem coverBlocksP : CoverBlocks.Goal := gen_coverBlocks model h0 hcyc hbins hmono


theorem cycleShiftP : CycleShift.Goal := hcyc

theorem constantWidth_refute : ¬ ConstantWidth.Goal := by
  intro h
  have h2 := h 2
  simp [model, span] at h2

theorem averageDivision_refute : ¬ AverageDivision.Goal := by
  intro h
  have h13 := h 13
  have hfast : fast model 13 = 1 := by
    have hlo : model.start 1 ≤ 13 := by simp [model]
    have hhi : 13 < model.start 2 := by simp [model]
    exact (predicateRangeP 13 1).mpr ⟨hlo, hhi⟩
  rw [hfast] at h13
  simp [model] at h13

theorem shortPeriodP : ShortPeriod.Goal := by
  intro n
  unfold span model
  simp
  omega

theorem oddBoundaryP : OddBoundary.Goal := by
  intro n
  have hlo : model.start (2*n+1) ≤ 28*n+13 := by simp [model]; omega
  have hhi : 28*n+13 < model.start (2*n+2) := by simp [model]; omega
  exact (predicateRangeP (28*n+13) (2*n+1)).mpr ⟨hlo, hhi⟩

end TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Facts_Payroll
