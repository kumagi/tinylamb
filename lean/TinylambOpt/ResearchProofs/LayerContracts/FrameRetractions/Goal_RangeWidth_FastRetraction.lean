import TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
import TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common_FrameRetractions

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_RangeWidth_FastRetraction

open TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
open TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common

theorem sound : RangeWidth.FastRetraction.Goal := by
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
  have hgt : 1 < xs.length := by
    have hp : 0 < xs.length := List.length_pos_of_mem hx
    omega
  have hne : xs ≠ [] := by intro h; subst xs; simp at hx
  have hminmem : xs.min?.getD 0 ∈ xs := by
    exact ((List.min?_eq_some_iff (xs := xs)).mp (min_eq_getD hne)).1
  have hmaxmem : xs.max?.getD 0 ∈ xs := by
    exact ((List.max?_eq_some_iff (xs := xs)).mp (max_eq_getD hne)).1
  have hle : xs.min?.getD 0 ≤ xs.max?.getD 0 := by
    exact ((List.max?_eq_some_iff (xs := xs)).mp (max_eq_getD hne)).2 _ hminmem
  by_cases hzero : xs.max?.getD 0 - xs.min?.getD 0 == 0
  · have hz : xs.max?.getD 0 - xs.min?.getD 0 = 0 := by
      have hz' : (xs.max?.getD 0 - xs.min?.getD 0 == 0) = true := hzero
      simpa using hz'
    have hzle : xs.max?.getD 0 ≤ xs.min?.getD 0 := Nat.sub_eq_zero_iff_le.mp hz
    have heq : xs.max?.getD 0 = xs.min?.getD 0 := Nat.le_antisymm hzle hle
    have hall : ∀ z, z ∈ xs → z = xs.min?.getD 0 := by
      intro z hz
      have hzmin : xs.min?.getD 0 ≤ z :=
        ((List.min?_eq_some_iff (xs := xs)).mp (min_eq_getD hne)).2 z hz
      have hzmax : z ≤ xs.max?.getD 0 :=
        ((List.max?_eq_some_iff (xs := xs)).mp (max_eq_getD hne)).2 z hz
      omega
    have hneerase : xs.erase x ≠ [] := by
      intro h
      have := congrArg List.length h
      rw [List.length_erase_of_mem hx] at this
      simp at this
      omega
    obtain ⟨y, ys', hcons⟩ := List.exists_cons_of_ne_nil hneerase
    have hy : y ∈ xs.erase x := by rw [hcons]; simp
    have hymem : y ∈ xs := List.mem_of_mem_erase hy
    have hymin : y = xs.min?.getD 0 := hall y hymem
    have hminer : (xs.erase x).min? = some (xs.min?.getD 0) := (List.min?_eq_some_iff).2 ⟨by
      rw [← hymin]
      exact hy, by
      intro z hz
      have hz' := hall z (List.mem_of_mem_erase hz)
      omega⟩
    have hmaxer : (xs.erase x).max? = some (xs.min?.getD 0) := (List.max?_eq_some_iff).2 ⟨by
      rw [← hymin]
      exact hy, by
      intro z hz
      have hz' := hall z (List.mem_of_mem_erase hz)
      omega⟩
    have hh : xs.max?.getD 0 - xs.min?.getD 0 = 0 ∧ some 0 = out := by
      simpa [dropOne, RangeWidth.model, hlen, hzero] using hd
    have hout : out = some 0 := hh.2.symm
    subst out
    have his := isEmpty_false_of_ne_nil hneerase
    simp [value, RangeWidth.model, his, hminer, hmaxer]
  · have hz : ¬ xs.max?.getD 0 - xs.min?.getD 0 == 0 := by
      cases h : xs.max?.getD 0 - xs.min?.getD 0 == 0 <;> simp_all
    have hh : xs.max?.getD 0 - xs.min?.getD 0 = 0 ∧ some 0 = out := by
      simpa [dropOne, RangeWidth.model, hlen] using hd
    have hb : (xs.max?.getD 0 - xs.min?.getD 0 == 0) = true := by simpa [hh.1]
    exact False.elim (hz hb)

end TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_RangeWidth_FastRetraction
