import TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
import TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common_FrameRetractions

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_FloorMean_DuplicateFrame

open TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
open TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common

theorem sound : FloorMean.DuplicateFrame.Goal := by
  intro xs
  by_cases h : xs = []
  · subst xs
    simp [value]
  have hne : (xs ++ xs) ≠ [] := by simp [h]
  rw [value, isEmpty_false_of_ne_nil hne]
  rw [value, isEmpty_false_of_ne_nil h]
  simp [FloorMean.model, List.sum_append]
  have hs : xs.sum + xs.sum = 2 * xs.sum := by omega
  have hl : xs.length + xs.length = 2 * xs.length := by omega
  rw [hs, hl, Nat.mul_div_mul_left xs.sum xs.length (by decide : 0 < 2)]

end TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_FloorMean_DuplicateFrame
