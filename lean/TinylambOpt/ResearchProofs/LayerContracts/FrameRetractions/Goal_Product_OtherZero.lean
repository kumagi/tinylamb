import TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
import TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common_FrameRetractions

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_Product_OtherZero

open TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
open TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common

theorem sound : Product.OtherZero.Goal := by
  intro xs x h0 hxpos hxmem
  have hgt : 1 < xs.length := by
    have h1 : 0 ∈ xs := h0
    have h2 : x ∈ xs := hxmem
    have hpos : 0 < xs.length := List.length_pos_of_mem h1
    by_cases hl1 : xs.length = 1
    · have hxs : xs = [0] := by
        cases xs with
        | nil => simp at hl1
        | cons a as =>
            simp at hl1
            subst as
            have : a = 0 := by simpa [eq_comm] using h1
            subst a
            rfl
      subst xs
      simp at hxmem
      omega
    · omega
  have hne0 : xs ≠ [] := by intro h; subst xs; simp at h0
  have hprod0 : xs.foldl (fun a b => a * b) 1 = 0 := by
    have hp := prod_eq_mul_prod_erase (xs := xs) (x := 0) h0
    rw [← List.prod_eq_foldl, hp]
    simp
  have hne : xs.erase x ≠ [] := by
    intro h
    have hl := congrArg List.length h
    rw [List.length_erase_of_mem hxmem] at hl
    simp at hl
    omega
  have hlenb : (xs.length == 1) = false := by
    cases hb : xs.length == 1
    · rfl
    · have hn : xs.length = 1 := by simpa using hb
      omega
  have hxne : x ≠ 0 := by omega
  simp [repaired, dropOne, Product.model, hlenb, hxne, hprod0, value,
    isEmpty_false_of_ne_nil hne]

end TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_Product_OtherZero
