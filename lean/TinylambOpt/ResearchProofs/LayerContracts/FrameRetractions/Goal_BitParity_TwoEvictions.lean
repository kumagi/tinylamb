import TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
import TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common_FrameRetractions

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_BitParity_TwoEvictions

open TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
open TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common

theorem sound : BitParity.TwoEvictions.Goal := by
  intro xs x y hx hy
  by_cases hxy : x = y
  · subst y
    rfl
  have hyin : y ∈ xs := List.mem_of_mem_erase hy
  have hxiny : x ∈ xs.erase y := (List.mem_erase_of_ne hxy).2 hx
  have hlenx : (xs.erase x).length = xs.length - 1 := List.length_erase_of_mem hx
  have hleny : (xs.erase y).length = xs.length - 1 := List.length_erase_of_mem hyin
  have hgtb : (xs.length == 1) = false := by
    cases hb : xs.length == 1
    · rfl
    · have hn : xs.length = 1 := by simpa using hb
      have hl : (xs.erase x).length = 0 := by rw [hlenx]; omega
      have hnil : xs.erase x = [] := List.length_eq_zero_iff.mp hl
      simp [hnil] at hy
  by_cases htwo : xs.length = 2
  · have lexn : (xs.erase x).length = 1 := by rw [hlenx]; omega
    have leyn : (xs.erase y).length = 1 := by rw [hleny]; omega
    have lex : (xs.erase x).length == 1 := by simp [lexn]
    have ley : (xs.erase y).length == 1 := by simp [leyn]
    simp [repaired, dropOne, BitParity.model, lex, ley]
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
    have h1 := xor_erase_of_mem hx
    have h2 := xor_erase_of_mem hyin
    have hf1 : (xs.erase x).foldl (fun a b => a ^^^ b) 0 =
        x ^^^ xs.foldl (fun a b => a ^^^ b) 0 := by
      rw [h1, ← Nat.xor_assoc, Nat.xor_self, Nat.zero_xor]
    have hf2 : (xs.erase y).foldl (fun a b => a ^^^ b) 0 =
        y ^^^ xs.foldl (fun a b => a ^^^ b) 0 := by
      rw [h2, ← Nat.xor_assoc, Nat.xor_self, Nat.zero_xor]
    simp [repaired, dropOne, BitParity.model, lex, ley, hf1, hf2]
    ac_rfl

end TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_BitParity_TwoEvictions
