import TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
import TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common_FrameRetractions

/-!
A sliding window can update aggregate state as a row leaves when the retained state
suffices to reconstruct the new result.

Proved

- `FloorMean.ExactDividend` — `∀ xs x, x ∈ xs → xs.sum%xs.length=0 → repaired model xs
  x = value model (xs.erase x)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_FloorMean_ExactDividend

open TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
open TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common

theorem sound : FloorMean.ExactDividend.Goal := by
  intro xs x hx hmod
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
    simp [repaired, dropOne, value]
  have hlenb : (xs.length == 1) = false := by
    cases hb : xs.length == 1
    · rfl
    · have hn : xs.length = 1 := by simpa using hb
      contradiction
  have hgt : 1 < xs.length := by
    have hp : 0 < xs.length := List.length_pos_of_mem hx
    omega
  have hdvd : xs.length ∣ xs.sum := Nat.dvd_of_mod_eq_zero hmod
  have hmul : xs.length * (xs.sum / xs.length) = xs.sum := by
    rw [Nat.mul_comm, Nat.div_mul_cancel hdvd]
  have hsx := sum_erase_of_mem hx
  have hlen_erase := List.length_erase_of_mem hx
  have hne : xs.erase x ≠ [] := by
    intro h
    have hl := congrArg List.length h
    rw [hlen_erase] at hl
    simp at hl
    omega
  have his := isEmpty_false_of_ne_nil hne
  simp [repaired, dropOne, FloorMean.model, hlenb, hmul]
  rw [← hsx]
  simp [value, FloorMean.model, his, hlen_erase]

end TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_FloorMean_ExactDividend
