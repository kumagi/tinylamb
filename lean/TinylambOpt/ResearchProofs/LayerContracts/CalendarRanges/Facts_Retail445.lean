import TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Common_CalendarRanges
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.YearsCommon

/-!
Date and timestamp predicates can use calendar bucket boundaries and range seeks where
the modeled endpoints agree.

Proved (8 claims, including the following)

- `Retail445.CoverBlocks` — Minimal bucket interval covering every day of a nonempty
  page range.
- `Retail445.CyclePopulation` — Analytical cardinality of a UNION of consecutive whole
  buckets.
- `Retail445.CycleShift` — Translate a bucket boundary by one calendar cycle.

Refuted by counterexample

- `Retail445.AverageDivision` — Can the optimizer replace calendar lookup by scaling
  with the mean width?
- `Retail445.ConstantWidth` — Can the JIT use a constant width learned from the first
  bucket?
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Facts_Retail445

open TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges
open TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges.Retail445
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
  have h3 := h 3
  simp [model, span] at h3

theorem averageDivision_refute : ¬ AverageDivision.Goal := by
  intro h
  have h28 := h 28
  have hfast : fast model 28 = 1 := by
    have hlo : model.start 1 ≤ 28 := by simp [model]
    have hhi : 28 < model.start 2 := by simp [model]
    exact (predicateRangeP 28 1).mpr ⟨hlo, hhi⟩
  rw [hfast] at h28
  simp [model] at h28

theorem fifthWeekP : FifthWeek.Goal := by
  intro q
  unfold span model
  simp
  omega

theorem fourthWeekP : FourthWeek.Goal := by
  intro q
  have hlo : model.start (3*q+1) ≤ 91*q+55 := by simp [model]; omega
  have hhi : 91*q+55 < model.start (3*q+2) := by simp [model]; omega
  exact (predicateRangeP (91*q+55) (3*q+1)).mpr ⟨hlo, hhi⟩

end TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Facts_Retail445
