import TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
import TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common_FrameRetractions

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_BitUnion_FastRetraction

open TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
open TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common

theorem sound : BitUnion.FastRetraction.Goal := by
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
  have hlenb : (xs.length == 1) = false := by
    cases hb : xs.length == 1
    · rfl
    · have hn : xs.length = 1 := by simpa using hb
      contradiction
  let v := xs.foldl (fun a b => a ||| b) 0
  have hh' := hd
  simp [dropOne, BitUnion.model, hlenb, v] at hh'
  have hh : (x = 0 ∨ v = 0) ∧ some v = out := by
    simpa [eq_comm] using hh'
  rw [← hh.2]
  have hor := or_erase_of_mem hx
  have hne : xs.erase x ≠ [] := by
    intro h
    have hl := congrArg List.length h
    rw [List.length_erase_of_mem hx] at hl
    simp at hl
    have hp : 0 < xs.length := List.length_pos_of_mem hx
    omega
  have his := isEmpty_false_of_ne_nil hne
  have htarget : (xs.erase x).foldl (fun a b => a ||| b) 0 = v := by
    dsimp [v]
    rcases hh.1 with hx0 | hv0
    · rw [hor, hx0]
      simp
    · dsimp [v] at hv0
      have hzero : x ||| (xs.erase x).foldl (fun a b => a ||| b) 0 = 0 := by
        rw [← hor]
        exact hv0
      rw [hv0]
      exact (Nat.or_eq_zero_iff.mp hzero).2
  simp [value, BitUnion.model, his, htarget]

end TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_BitUnion_FastRetraction
