import TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
import TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common_LowerMedian

/-!
A sliding window can update aggregate state as a row leaves when the retained state
suffices to reconstruct the new result.

Refuted by counterexample

- `LowerMedian.PhysicalCount` — Can a nullable frame pass physical length instead of
  non-NULL population to the inverse?
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_LowerMedian_PhysicalCount

open TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
open TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.CommonLowerMedian
open List

theorem med4 : lowerMedian [0, 1, 2, 3] = 1 := by
  apply lowerMedian_eq_of_bounds (t := 1) (m := 1)
  · decide
  · decide
  · decide
  · decide

theorem med3 : lowerMedian [1, 2, 3] = 2 := by
  apply lowerMedian_eq_of_bounds (t := 1) (m := 2)
  · decide
  · decide
  · decide
  · decide

/-- Counterexample: five physical rows, four non-NULL values. -/
theorem refute : ¬ LowerMedian.PhysicalCount.Goal := by
  intro h
  let rows : List (Option Nat) := [some 0, some 1, some 2, some 3, none]
  have hfilter : rows.filterMap (id : Option Nat → Option Nat) = [0, 1, 2, 3] := by rfl
  have hpremise : LowerMedian.model.remove rows.length
      (LowerMedian.model.result (rows.filterMap (id : Option Nat → Option Nat))) 0 = some 1 := by
    simp [LowerMedian.model, rows, hfilter, med4]
  have hh := h rows 0 1 (by simp [rows]) (by decide) hpremise
  have hfilter2 : (rows.erase (some 0)).filterMap (id : Option Nat → Option Nat) = [1, 2, 3] := by rfl
  have hbad : value LowerMedian.model ((rows.erase (some 0)).filterMap (id : Option Nat → Option Nat)) ≠
      (some 1 : Option Nat) := by
    simp [value, LowerMedian.model, hfilter2, med3]
  exact hbad hh

end TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_LowerMedian_PhysicalCount
