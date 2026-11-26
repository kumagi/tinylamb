import TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
import TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common_FrameRetractions

/-!
A sliding window can update aggregate state as a row leaves when the retained state
suffices to reconstruct the new result.

Proved

- `BitUnion.DuplicateFrame` — Can an executor collapse a duplicated whole peer frame
  before finalizing?
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_BitUnion_DuplicateFrame

open TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
open TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common

theorem sound : BitUnion.DuplicateFrame.Goal := by
  intro xs
  by_cases h : xs = []
  · subst xs
    simp [value]
  have hne : (xs ++ xs) ≠ [] := by simp [h]
  rw [value, isEmpty_false_of_ne_nil hne]
  rw [value, isEmpty_false_of_ne_nil h]
  simp only [Bool.false_eq_true, ite_false, Option.some.injEq]
  change ((xs ++ xs).foldl (fun a b => a ||| b) 0) = xs.foldl (fun a b => a ||| b) 0
  rw [foldl_or_append]
  rw [foldl_or_acc]
  simp [Nat.or_self]

end TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_BitUnion_DuplicateFrame
