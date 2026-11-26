import TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
import TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common_LowerMedian

/-!
A sliding window can update aggregate state as a row leaves when the retained state
suffices to reconstruct the new result.

Proved

- `LowerMedian.LowerOfTwo` — `∀ x y, value model [x,y] = some (min x y)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_LowerMedian_LowerOfTwo

open TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
open List

theorem mergeSort_two (x y : Nat) : [x, y].mergeSort = [min x y, max x y] := by
  rw [mergeSort_pair]
  by_cases h : x ≤ y
  · simp [h, Nat.min_eq_left h, Nat.max_eq_right h]
  · simp [h, Nat.min_eq_right (Nat.le_of_not_le h), Nat.max_eq_left (Nat.le_of_not_le h)]

theorem getD_eq_getElem {xs : List Nat} {n d : Nat} (h : n < xs.length) :
    xs[n]?.getD d = xs[n] := by
  have h2 : xs[n]? = some xs[n] := (List.getElem?_eq_some_getElem_iff h).mpr trivial
  rw [h2, Option.getD_some]

theorem sound : LowerMedian.LowerOfTwo.Goal := by
  intro x y
  show (if ([x, y] : List Nat).isEmpty = true then none else some (lowerMedian [x, y]))
      = some (min x y)
  rw [if_neg (by simp : ¬ (([x, y] : List Nat).isEmpty = true))]
  apply Option.some.inj
  unfold lowerMedian
  rw [mergeSort_two, show ((([x, y].length - 1) / 2 : Nat)) = 0 from by simp]
  rw [getD_eq_getElem (by simp : (0 : Nat) < [min x y, max x y].length)]
  simp

end TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_LowerMedian_LowerOfTwo
