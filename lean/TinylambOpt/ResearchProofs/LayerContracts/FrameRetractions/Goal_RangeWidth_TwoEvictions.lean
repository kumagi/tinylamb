import TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
import TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common_FrameRetractions

/-!
A sliding window can update aggregate state as a row leaves when the retained state
suffices to reconstruct the new result.

Proved

- `RangeWidth.TwoEvictions` — Repaired deletions in either order agree, including
  repeated outgoing values.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_RangeWidth_TwoEvictions

open TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
open TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common

theorem repaired_eq_value {xs : List Nat} {x : Nat} (hx : x ∈ xs) :
    repaired RangeWidth.model xs x = value RangeWidth.model (xs.erase x) := by
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
      cases h : xs.length == 1 <;> simp_all
    by_cases hz : xs.max?.getD 0 - xs.min?.getD 0 == 0
    · have hne0 : xs ≠ [] := by intro h; subst xs; simp at hx
      have hminmem : xs.min?.getD 0 ∈ xs := by
        exact ((List.min?_eq_some_iff (xs := xs)).mp (min_eq_getD hne0)).1
      have hle : xs.min?.getD 0 ≤ xs.max?.getD 0 := by
        exact ((List.max?_eq_some_iff (xs := xs)).mp (max_eq_getD hne0)).2 _ hminmem
      have hz0 : xs.max?.getD 0 - xs.min?.getD 0 = 0 := by
        have hz' : (xs.max?.getD 0 - xs.min?.getD 0 == 0) = true := hz
        simpa using hz'
      have heq : xs.max?.getD 0 = xs.min?.getD 0 :=
        Nat.le_antisymm (Nat.sub_eq_zero_iff_le.mp hz0) hle
      have hall : ∀ z, z ∈ xs → z = xs.min?.getD 0 := by
        intro z hzmem
        have hzmin : xs.min?.getD 0 ≤ z :=
          ((List.min?_eq_some_iff (xs := xs)).mp (min_eq_getD hne0)).2 z hzmem
        have hzmax : z ≤ xs.max?.getD 0 :=
          ((List.max?_eq_some_iff (xs := xs)).mp (max_eq_getD hne0)).2 z hzmem
        omega
      have hneerase : xs.erase x ≠ [] := by
        intro h
        have := congrArg List.length h
        rw [List.length_erase_of_mem hx] at this
        simp at this
        have hp : 0 < xs.length := List.length_pos_of_mem hx
        by_cases hl1 : xs.length = 1
        · have : (xs.length == 1) = true := by simpa [hl1]
          exact hlen this
        · omega
      obtain ⟨y, ys', hcons⟩ := List.exists_cons_of_ne_nil hneerase
      have hymem : y ∈ xs.erase x := by rw [hcons]; simp
      have hymin : y = xs.min?.getD 0 := hall y (List.mem_of_mem_erase hymem)
      have hminer : (xs.erase x).min? = some (xs.min?.getD 0) := (List.min?_eq_some_iff).2 ⟨by
        rw [← hymin]; exact hymem, by
        intro z hzmem
        have hz' := hall z (List.mem_of_mem_erase hzmem)
        omega⟩
      have hmaxer : (xs.erase x).max? = some (xs.min?.getD 0) := (List.max?_eq_some_iff).2 ⟨by
        rw [← hymin]; exact hymem, by
        intro z hzmem
        have hz' := hall z (List.mem_of_mem_erase hzmem)
        omega⟩
      have hval : value RangeWidth.model (xs.erase x) = some 0 := by
        have his := isEmpty_false_of_ne_nil hneerase
        simp [value, RangeWidth.model, his, hminer, hmaxer]
      have hzb : (xs.max?.getD 0 - xs.min?.getD 0 == 0) = true := hz
      simp [repaired, dropOne, RangeWidth.model, hlenb, hzb, hz0]
      exact hval.symm
    · have hzb : (xs.max?.getD 0 - xs.min?.getD 0 == 0) = false := by
        cases h : xs.max?.getD 0 - xs.min?.getD 0 == 0 <;> simp_all
      have hz0neg : ¬ xs.max?.getD 0 - xs.min?.getD 0 = 0 := by
        intro h0
        have ht : (xs.max?.getD 0 - xs.min?.getD 0 == 0) = true := by simp [h0]
        rw [ht] at hzb
        contradiction
      simp [repaired, dropOne, RangeWidth.model, hlenb, hzb, hz0neg]

theorem sound : RangeWidth.TwoEvictions.Goal := by
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
    have hmin := perm_min hp
    have hmax := perm_max hp
    simp [value, RangeWidth.model, isEmpty_false_of_ne_nil hne1,
      isEmpty_false_of_ne_nil hne2, hmin, hmax]

end TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_RangeWidth_TwoEvictions
