import TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
import TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common_FrameRetractions

/-!
A sliding window can update aggregate state as a row leaves when the retained state
suffices to reconstruct the new result.

Proved

- `Sum.RemoveZero` — `∀ xs, 0 ∈ xs → 1 < xs.length → repaired model xs 0 = value model
  xs`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_Sum_RemoveZero

open TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
open TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common

theorem sound : Sum.RemoveZero.Goal := by
  intro xs hx hgt
  have hne_xs : xs ≠ [] := by intro h; subst xs; simp at hx
  have hne_erase : xs.erase 0 ≠ [] := by
    intro h
    have := congrArg List.length h
    rw [List.length_erase_of_mem hx] at this
    simp at this
    omega
  have hne1 : (xs.length == 1) = false := by
    cases hb : xs.length == 1
    · rfl
    · have hn : xs.length = 1 := by simpa using hb
      omega
  have his_xs := isEmpty_false_of_ne_nil hne_xs
  have his_erase := isEmpty_false_of_ne_nil hne_erase
  simp [repaired, dropOne, Sum.model, hne1, value, his_xs, his_erase]

end TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_Sum_RemoveZero
