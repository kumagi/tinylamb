import TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
import TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common_FrameRetractions

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_Product_TwoEvictions

open TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
open TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common

theorem repaired_eq_value {xs : List Nat} {x : Nat} (hx : x ∈ xs) :
    repaired Product.model xs x = value Product.model (xs.erase x) := by
  by_cases hlen : xs.length == 1
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
  · have hlenb : (xs.length == 1) = false := by
      cases hb : xs.length == 1
      · rfl
      · have hn : xs.length = 1 := by simpa using hb
        exact False.elim (hlen hb)
    by_cases hx0 : x == 0
    · have hx0b : x = 0 := by simpa using hx0
      subst x
      simp [repaired, dropOne, Product.model, hlenb]
    · have hxne : x ≠ 0 := by intro h; subst x; simp at hx0
      have hp := prod_eq_mul_prod_erase hx
      have hdiv : xs.prod / x = (xs.erase x).prod := by
        rw [hp, Nat.mul_div_right _ (Nat.pos_of_ne_zero hxne)]
      have hdivl : xs.foldl (fun a b => a * b) 1 / x =
          (xs.erase x).foldl (fun a b => a * b) 1 := by
        simpa [List.prod_eq_foldl] using hdiv
      have hx0b : (x == 0) = false := by
        cases hb : x == 0
        · rfl
        · have : x = 0 := by simpa using hb
          exact False.elim (hxne this)
      have hne : xs.erase x ≠ [] := by
        intro h
        have hl := congrArg List.length h
        rw [List.length_erase_of_mem hx] at hl
        simp at hl
        have hp : 0 < xs.length := List.length_pos_of_mem hx
        have hl1 : xs.length = 1 := by omega
        exact hlen (by simpa [hl1])
      have his := isEmpty_false_of_ne_nil hne
      simp [repaired, dropOne, Product.model, hlenb, hx0b, hxne, hdivl,
        value, his, List.prod_eq_foldl]

theorem sound : Product.TwoEvictions.Goal := by
  intro xs x y hx hy
  by_cases hxy : x = y
  · subst y
    rfl
  have hyin : y ∈ xs := List.mem_of_mem_erase hy
  have hxiny : x ∈ xs.erase y := (List.mem_erase_of_ne hxy).2 hx
  have hlenx : (xs.erase x).length = xs.length - 1 := List.length_erase_of_mem hx
  have hleny : (xs.erase y).length = xs.length - 1 := List.length_erase_of_mem hyin
  by_cases htwo : xs.length = 2
  · have lexn : (xs.erase x).length = 1 := by rw [hlenx]; omega
    have leyn : (xs.erase y).length = 1 := by rw [hleny]; omega
    have lex : (xs.erase x).length == 1 := by simpa [lexn]
    have ley : (xs.erase y).length == 1 := by simpa [leyn]
    simp [repaired, dropOne, lex, ley]
  · have hgt3 : 3 ≤ xs.length := by
      have hp : 0 < (xs.erase x).length := List.length_pos_of_mem hy
      have hnpos : 0 < xs.length := List.length_pos_of_mem hx
      omega
    rw [repaired_eq_value hy, repaired_eq_value hxiny]
    have hne1 : (xs.erase x).erase y ≠ [] := by
      intro h
      have : ((xs.erase x).erase y).length = 0 := by simp [h]
      rw [List.length_erase_of_mem hy, hlenx] at this
      omega
    have hne2 : (xs.erase y).erase x ≠ [] := by
      intro h
      have : ((xs.erase y).erase x).length = 0 := by simp [h]
      rw [List.length_erase_of_mem hxiny, hleny] at this
      omega
    have hp := erase_erase_perm xs x y hx hy
    have hprod := perm_prod hp
    have hprodl : ((xs.erase x).erase y).foldl (fun a b => a * b) 1 =
        ((xs.erase y).erase x).foldl (fun a b => a * b) 1 := by
      simpa [List.prod_eq_foldl] using hprod
    simp [value, Product.model, isEmpty_false_of_ne_nil hne1,
      isEmpty_false_of_ne_nil hne2, hprodl]

end TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_Product_TwoEvictions
