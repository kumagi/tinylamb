import TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
import TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common_FrameRetractions

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_Sum_TwoEvictions

open TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
open TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common

theorem sound : Sum.TwoEvictions.Goal := by
  intro xs x y hx hy
  by_cases hxy : x = y
  · subst y
    rfl
  have hyin : y ∈ xs := List.mem_of_mem_erase hy
  have hxiny : x ∈ xs.erase y := (List.mem_erase_of_ne hxy).2 hx
  have hlenx := List.length_erase_of_mem hx
  have hleny := List.length_erase_of_mem hyin
  have hne : xs.erase x ≠ [] := by
    intro h
    have hl : (xs.erase x).length = 0 := by simp [h]
    simp [h] at hy
  have hgtb : (xs.length == 1) = false := by
    cases hb : xs.length == 1
    · rfl
    · have : xs.length = 1 := by simpa using hb
      have hl : (xs.erase x).length = 0 := by rw [hlenx]; omega
      exact False.elim (hne (List.length_eq_zero_iff.mp hl))
  by_cases htwo : xs.length = 2
  · have lexn : (xs.erase x).length = 1 := by rw [hlenx]; omega
    have leyn : (xs.erase y).length = 1 := by rw [hleny]; omega
    have lex : (xs.erase x).length == 1 := by simpa [lexn]
    have ley : (xs.erase y).length == 1 := by simpa [leyn]
    simp [repaired, dropOne, Sum.model, hgtb, lex, ley]
  · have lexn : ¬ (xs.erase x).length = 1 := by rw [hlenx]; omega
    have leyn : ¬ (xs.erase y).length = 1 := by rw [hleny]; omega
    have lex : ((xs.erase x).length == 1) = false := by
      cases hb : (xs.erase x).length == 1
      · rfl
      · have : (xs.erase x).length = 1 := by simpa using hb
        exact False.elim (lexn this)
    have ley : ((xs.erase y).length == 1) = false := by
      cases hb : (xs.erase y).length == 1
      · rfl
      · have : (xs.erase y).length = 1 := by simpa using hb
        exact False.elim (leyn this)
    have hsx := sum_erase_of_mem hx
    have hsy := sum_erase_of_mem hyin
    have hsxy := sum_erase_of_mem (x := x) hxiny
    have hs1 := sum_erase_of_mem (xs := xs.erase x) (x := y) hy
    simp [repaired, dropOne, Sum.model, lex, ley, hs1, hsxy]
    omega

end TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_Sum_TwoEvictions
