import TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
import TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common_FrameRetractions

/-!
A sliding window can update aggregate state as a row leaves when the retained state
suffices to reconstruct the new result.

Proved

- `Product.FastRetraction` — Every accepted constant-state deletion agrees with
  recomputing the remaining bag.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_Product_FastRetraction

open TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
open TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common

theorem sound : Product.FastRetraction.Goal := by
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
  have hxne : x ≠ 0 := by
    by_cases hx0 : x = 0
    · subst x
      simp [dropOne, Product.model, hlenb] at hd
    · exact hx0
  have hh : ∃ a, (x ≠ 0 ∧ xs.prod / x = a) ∧ some a = out := by
    simpa [dropOne, Product.model, hlenb, List.prod_eq_foldl] using hd
  rcases hh with ⟨a, ⟨_, ha⟩, hs⟩
  have hp := prod_eq_mul_prod_erase hx
  have hdiv : xs.prod / x = (xs.erase x).prod := by
    rw [hp, Nat.mul_div_right _ (Nat.pos_of_ne_zero hxne)]
  have hne : xs.erase x ≠ [] := by
    intro h
    have hl := congrArg List.length h
    rw [List.length_erase_of_mem hx] at hl
    simp at hl
    have hp : 0 < xs.length := List.length_pos_of_mem hx
    omega
  have his := isEmpty_false_of_ne_nil hne
  rw [← hs]
  have hdiva : a = (xs.erase x).prod := by rw [← ha, hdiv]
  rw [hdiva]
  simp [value, Product.model, his, List.prod_eq_foldl]

end TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_Product_FastRetraction
