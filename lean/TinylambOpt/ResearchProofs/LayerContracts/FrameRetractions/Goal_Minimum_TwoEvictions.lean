import TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
import TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common_FrameRetractions

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_Minimum_TwoEvictions

open TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
open TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common

theorem erase_erase_perm' (xs : List Nat) (x y : Nat) (hx : x ∈ xs) (hy : y ∈ xs.erase x) :
    ((xs.erase x).erase y).Perm ((xs.erase y).erase x) := by
  induction xs with
  | nil => simp at hx
  | cons z zs ih =>
      by_cases hz : z == x
      · have hzx : z = x := by simpa using hz
        subst x
        simp only [List.erase_cons_head] at hy ⊢
        by_cases hyz : y == z
        · have hyz' : y = z := by simpa using hyz
          subst y
          simp
        · have hzy : ¬ z == y := by
            cases h : z == y <;> simp_all
          rw [List.erase_cons_tail hzy]
          simp
      · have hzx : z ≠ x := by intro h; subst x; simp at hz
        have hxt : x ∈ zs := by
          simp only [List.mem_cons] at hx
          cases hx with
          | inl h => exact False.elim (hzx h.symm)
          | inr h => exact h
        by_cases hyz : y == z
        · have hyz' : y = z := by simpa using hyz
          subst y
          rw [List.erase_cons_tail hz]
          simp
        · rw [List.erase_cons_tail hz] at hy
          have hyt : y ∈ zs.erase x := by
            simp only [List.mem_cons] at hy
            cases hy with
            | inl h =>
                have hyne : y ≠ z := by intro hh; subst y; simp at hyz
                exact False.elim (hyne h)
            | inr h => exact h
          have ihp := ih hxt hyt
          have hzy : ¬ z == y := by
            cases h : z == y <;> simp_all
          simp only [List.erase_cons_tail hz, List.erase_cons_tail hzy]
          exact ihp.cons z
theorem minRepair {xs : List Nat} {x : Nat} (hgt : (xs.length == 1) = false)
    (hne : xs.erase x ≠ []) :
    repaired Minimum.model xs x = some ((xs.erase x).min?.getD 0) := by
  by_cases hlt : xs.min?.getD 0 < x
  · by_cases hx : x ∈ xs
    · simp [repaired, dropOne, Minimum.model, hgt, hlt, min_erase_of_lt hx hlt]
    · have her : xs.erase x = xs := List.erase_eq_self_iff.mpr hx
      simp [repaired, dropOne, Minimum.model, hgt, hlt, her]
  · simp [repaired, dropOne, Minimum.model, hgt, hlt, value, isEmpty_false_of_ne_nil hne]

theorem sound : Minimum.TwoEvictions.Goal := by
  intro xs x y hx hy
  by_cases hxy : x = y
  · subst y
    rfl
  have hyin : y ∈ xs := List.mem_of_mem_erase hy
  have hxiny : x ∈ xs.erase y := (List.mem_erase_of_ne hxy).2 hx
  have hlenx : (xs.erase x).length = xs.length - 1 := List.length_erase_of_mem hx
  have hleny : (xs.erase y).length = xs.length - 1 := List.length_erase_of_mem hyin
  have hne : xs.erase x ≠ [] := by
    intro h
    have : (xs.erase x).length = 0 := by simp [h]
    simp [h] at hy
  have hgtb : (xs.length == 1) = false := by
    cases hb : xs.length == 1
    · rfl
    · have hn : xs.length = 1 := by simpa using hb
      have hl : (xs.erase x).length = 0 := by rw [hlenx]; omega
      exact False.elim (hne (List.length_eq_zero_iff.mp hl))
  by_cases htwo : xs.length = 2
  · have lexn : (xs.erase x).length = 1 := by rw [hlenx]; omega
    have leyn : (xs.erase y).length = 1 := by rw [hleny]; omega
    have lex : (xs.erase x).length == 1 := by simp [lexn]
    have ley : (xs.erase y).length == 1 := by simp [leyn]
    simp [repaired, dropOne, Minimum.model, lex, ley]
  · have lex : ((xs.erase x).length == 1) = false := by
      cases hb : (xs.erase x).length == 1
      · rfl
      · have hn : (xs.erase x).length = 1 := by simpa using hb
        have hn' : xs.length = 2 := by rw [hlenx] at hn; omega
        exact False.elim (htwo hn')
    have ley : ((xs.erase y).length == 1) = false := by
      cases hb : (xs.erase y).length == 1
      · rfl
      · have hn : (xs.erase y).length = 1 := by simpa using hb
        have hn' : xs.length = 2 := by rw [hleny] at hn; omega
        exact False.elim (htwo hn')
    have hpos1 : 0 < (xs.erase x).length := List.length_pos_of_mem hy
    have hgt3 : 3 ≤ xs.length := by omega
    have hne1 : (xs.erase x).erase y ≠ [] := by
      intro h
      have : ((xs.erase x).erase y).length = 0 := by simp [h]
      rw [List.length_erase_of_mem hy, hlenx] at this
      omega
    have hrep1 := minRepair lex hne1
    have hne_y : xs.erase y ≠ [] := by
      intro h
      have : (xs.erase y).length = 0 := by simp [h]
      simp [h] at hxiny
    have hne2 : (xs.erase y).erase x ≠ [] := by
      intro h
      have : ((xs.erase y).erase x).length = 0 := by simp [h]
      rw [List.length_erase_of_mem hxiny, hleny] at this
      omega
    have hrep2 := minRepair ley hne2
    rw [hrep1, hrep2]
    have hp := perm_min (erase_erase_perm' xs x y hx hy)
    exact congrArg some hp

end TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_Minimum_TwoEvictions
