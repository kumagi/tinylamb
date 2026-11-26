import TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
import TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common_FrameRetractions

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_Minimum_FastRetraction

open TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
open TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common

theorem sound : Minimum.FastRetraction.Goal := by
  intro xs x out hx hd
  by_cases hlen : xs.length = 1
  · have hxs : xs = [x] := by
      cases xs with
      | nil => simp at hlen
      | cons y ys =>
          simp at hlen
          subst ys
          have hxy : x = y := by simpa using hx
          subst y
          rfl
    subst xs
    simp [dropOne, value] at hd ⊢
    exact hd
  · have hgt : 1 < xs.length := by
      have hp : 0 < xs.length := List.length_pos_of_mem hx
      omega
    have hne : xs.erase x ≠ [] := by
      intro h
      have := congrArg List.length h
      rw [List.length_erase_of_mem hx] at this
      simp at this
      omega
    by_cases hlt : xs.min?.getD 0 < x
    · have hout : out = some (xs.min?.getD 0) := by
        have hh : some (xs.min?.getD 0) = out := by
          simpa [dropOne, Minimum.model, hlen, hlt] using hd
        exact hh.symm
      subst out
      have he := min_erase_of_lt hx hlt
      have his := isEmpty_false_of_ne_nil hne
      simp [value, Minimum.model, his, he]
    · simp [dropOne, Minimum.model, hlen, hlt] at hd

end TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_Minimum_FastRetraction
