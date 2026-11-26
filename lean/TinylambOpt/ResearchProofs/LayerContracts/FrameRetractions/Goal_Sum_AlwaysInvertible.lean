import TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
import TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common_FrameRetractions

/-!
A sliding window can update aggregate state as a row leaves when the retained state
suffices to reconstruct the new result.

Proved

- `Sum.AlwaysInvertible` — Does a finalized scalar and non-NULL count determine every
  single-row retraction?
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_Sum_AlwaysInvertible

open TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
open TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common

theorem sound : Sum.AlwaysInvertible.Goal := by
  intro xs ys x hx hy hlen hv
  by_cases hsingle : xs.length = 1
  · have hxempty : xs.erase x = [] := by
      apply List.length_eq_zero_iff.mp
      rw [List.length_erase_of_mem hx]
      omega
    have hyempty : ys.erase x = [] := by
      apply List.length_eq_zero_iff.mp
      rw [List.length_erase_of_mem hy]
      omega
    simp [value, hxempty, hyempty]
  · have hgt : 1 < xs.length := by
      have hp : 0 < xs.length := List.length_pos_of_mem hx
      omega
    have hv' : xs.sum = ys.sum := by
      have hne_x : xs ≠ [] := by intro h; subst xs; simp at hx
      have hne_y : ys ≠ [] := by
        intro h; subst ys
        contradiction
      simpa [value, Sum.model, hne_x, hne_y] using hv
    have hx1 := sum_erase_of_mem hx
    have hy1 := sum_erase_of_mem hy
    have hne_x : xs.erase x ≠ [] := by
      intro h
      have := congrArg List.length h
      rw [List.length_erase_of_mem hx] at this
      simp at this
      omega
    have hne_y : ys.erase x ≠ [] := by
      intro h
      have := congrArg List.length h
      rw [List.length_erase_of_mem hy] at this
      simp at this
      omega
    simp [value, Sum.model, hne_x, hne_y, hx1, hy1, hv']

end TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_Sum_AlwaysInvertible
