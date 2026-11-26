import TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
import TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common_FrameRetractions

/-!
A sliding window can update aggregate state as a row leaves when the retained state
suffices to reconstruct the new result.

Proved

- `DistinctCount.DuplicateFrame` — Can an executor collapse a duplicated whole peer
  frame before finalizing?
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_DistinctCount_DuplicateFrame

open TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
open TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common

theorem sound : DistinctCount.DuplicateFrame.Goal := by
  intro xs
  by_cases h : xs = []
  · subst xs
    simp [value]
  have hdup : xs.removeAll xs = [] := removeAll_eq_nil_of_subset (by intro z hz; exact hz)
  have hne : (xs ++ xs) ≠ [] := by simp [h]
  rw [value, isEmpty_false_of_ne_nil hne]
  rw [value, isEmpty_false_of_ne_nil h]
  simp only [Bool.false_eq_true, ite_false, Option.some.injEq]
  change (xs ++ xs).eraseDups.length = xs.eraseDups.length
  rw [List.eraseDups_append, hdup]
  simp

end TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_DistinctCount_DuplicateFrame
