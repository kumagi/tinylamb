import TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
import TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common_FrameRetractions

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_BitUnion_TwoEvictions

open TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
open TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common

theorem or_erase_eq {xs : List Nat} {x : Nat} (hx : x ∈ xs)
    (h : x = 0 ∨ xs.foldl (fun a b => a ||| b) 0 = 0) :
    (xs.erase x).foldl (fun a b => a ||| b) 0 = xs.foldl (fun a b => a ||| b) 0 := by
  have hor := or_erase_of_mem hx
  rcases h with rfl | hv0
  · rw [hor]
    simp
  · have hz : x ||| (xs.erase x).foldl (fun a b => a ||| b) 0 = 0 := by rw [← hor]; exact hv0
    rw [hv0]
    exact (Nat.or_eq_zero_iff.mp hz).2

theorem repaired_eq_value {xs : List Nat} {x : Nat} (hx : x ∈ xs) :
    repaired BitUnion.model xs x = value BitUnion.model (xs.erase x) := by
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
    have hne : xs.erase x ≠ [] := by
      intro h
      have hl := congrArg List.length h
      rw [List.length_erase_of_mem hx] at hl
      simp at hl
      have hp : 0 < xs.length := List.length_pos_of_mem hx
      have hn1 : xs.length = 1 := by omega
      have hbtrue : (xs.length == 1) = true := by simpa [hn1]
      rw [hbtrue] at hlenb
      contradiction
    have his := isEmpty_false_of_ne_nil hne
    by_cases hx0 : x == 0
    · have hxzero : x = 0 := by simpa using hx0
      have ht := or_erase_eq hx (Or.inl hxzero)
      have hcond : x = 0 ∨ xs.foldl (fun a b => a ||| b) 0 = 0 := Or.inl hxzero
      simp [repaired, dropOne, BitUnion.model, hlenb, hcond]
      rw [← ht]
      simp [value, BitUnion.model, his]
    · by_cases hv0 : xs.foldl (fun a b => a ||| b) 0 == 0
      · have hvzero : xs.foldl (fun a b => a ||| b) 0 = 0 := by simpa using hv0
        have ht := or_erase_eq hx (Or.inr hvzero)
        have hcond : x = 0 ∨ xs.foldl (fun a b => a ||| b) 0 = 0 := Or.inr hvzero
        simp [repaired, dropOne, BitUnion.model, hlenb, hcond]
        rw [← ht]
        simp [value, BitUnion.model, his]
      · have hxne : x ≠ 0 := by intro h; subst x; simp at hx0
        have hcond : ¬ (x = 0 ∨ xs.foldl (fun a b => a ||| b) 0 = 0) := by
          intro hz
          rcases hz with hz | hz
          · exact hxne hz
          · have hzb : (xs.foldl (fun a b => a ||| b) 0 == 0) = true := by simp [hz]
            rw [hzb] at hv0
            contradiction
        simp [repaired, dropOne, BitUnion.model, hlenb, hcond, value, his]

theorem sound : BitUnion.TwoEvictions.Goal := by
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
    have hor := perm_foldl_or hp
    simp [value, BitUnion.model, isEmpty_false_of_ne_nil hne1,
      isEmpty_false_of_ne_nil hne2, hor]

end TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_BitUnion_TwoEvictions
