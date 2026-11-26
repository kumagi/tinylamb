import TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
import TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common_LowerMedian

/-!
A sliding window can update aggregate state as a row leaves when the retained state
suffices to reconstruct the new result.

Refuted by counterexample

- `LowerMedian.FallbackNecessary` — A rejected deletion has indistinguishable old
  summaries requiring different results.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_LowerMedian_FallbackNecessary

open TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
open TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.CommonLowerMedian
open List

theorem med000 : lowerMedian [0, 0, 0] = 0 := by
  apply lowerMedian_eq_of_bounds (t := 1) (m := 0)
  · decide
  · decide
  · decide
  · decide

theorem med00 : lowerMedian [0, 0] = 0 := by
  rw [lowerMedian_pair]
  simp

/-- `[0,0,0]` rejects the outgoing 0, but every length-3 peer with value 0
    erases to a bag whose value is again 0, so no rescan witness exists. -/
theorem refute : ¬ LowerMedian.FallbackNecessary.Goal := by
  intro h
  have hdrop : dropOne LowerMedian.model [0, 0, 0] 0 = none := by
    simp [dropOne, LowerMedian.model, med000]
  obtain ⟨ys, hym, hylen, hyval, hdiff⟩ := h [0, 0, 0] 0 (by simp) hdrop
  have hlen3 : ys.length = 3 := by simpa using hylen
  have hpos : 0 < ys.length := by omega
  have hne : ys ≠ [] := by
    intro hnil
    simp [hnil] at hlen3
  have hv000 : value LowerMedian.model ([0, 0, 0] : List Nat) = some 0 := by
    simp [value, LowerMedian.model, med000]
  have hysmed : lowerMedian ys = 0 := by
    rw [hv000] at hyval
    simpa [value, LowerMedian.model, List.isEmpty_eq_false_iff.mpr hne] using hyval
  obtain ⟨_, h2b⟩ := bounds hpos hysmed
  rw [hlen3] at h2b
  have hP0 : (fun z => decide (z ≤ 0)) (0 : Nat) = true := by decide
  have hcbase := countP_erase_of_mem (P := fun z => decide (z ≤ 0)) hym
  simp only [decide_eq_true_eq, Nat.le_refl, if_true] at hcbase
  have hcpos : 0 < (ys.erase 0).countP (fun z => decide (z ≤ 0)) := by omega
  rw [List.countP_eq_length_filter] at hcpos
  have hfilne : (ys.erase 0).filter (fun z => decide (z ≤ 0)) ≠ [] :=
    (List.length_pos_iff).mp hcpos
  obtain ⟨z, hz⟩ := List.exists_mem_of_ne_nil _ hfilne
  have hzmem : z ∈ ys.erase 0 := (List.mem_filter.mp hz).1
  have hzle : z ≤ 0 := by
    have := (List.mem_filter.mp hz).2
    simpa using this
  have hz0 : z = 0 := Nat.eq_zero_of_le_zero hzle
  have hzero_in : 0 ∈ ys.erase 0 := by simpa [hz0] using hzmem
  have hmedzs : lowerMedian (ys.erase 0) = 0 := by
    let zs := ys.erase 0
    have hlen2 : zs.length = 2 := by
      have := List.length_erase_of_mem hym
      rw [hlen3] at this
      simpa [zs] using this
    have hzs0 : 0 ∈ zs := by simpa [zs] using hzero_in
    change lowerMedian zs = 0
    cases hzs : zs with
    | nil => simp [hzs] at hlen2
    | cons a t =>
        cases ht : t with
        | nil => simp [hzs, ht] at hlen2
        | cons b u =>
            have hu : u = [] := by simpa [hzs, ht] using hlen2
            subst u
            have hzab : 0 = a ∨ 0 = b := by simpa [hzs, ht] using hzs0
            rcases hzab with hza | hzb
            · subst a
              rw [lowerMedian_pair]
              simp
            · have hb0 : b = 0 := hzb.symm
              subst b
              have hp : [a, 0].Perm [0, a] := (List.Perm.swap a 0 []).symm
              rw [lowerMedian_of_perm hp, lowerMedian_pair]
              simp
  have hnzs : ys.erase 0 ≠ [] := by
    intro hnil
    have hlen2 := List.length_erase_of_mem hym
    rw [hlen3, hnil] at hlen2
    simp at hlen2
  have hvalzs : value LowerMedian.model (ys.erase 0) = some 0 := by
    simp [value, LowerMedian.model, List.isEmpty_eq_false_iff.mpr hnzs, hmedzs]
  have herase : ([0, 0, 0] : List Nat).erase 0 = [0, 0] := by rfl
  have hvalxs : value LowerMedian.model ([0, 0, 0].erase 0) = some 0 := by
    simp [value, LowerMedian.model, herase, med00]
  rw [hvalxs] at hdiff
  exact hdiff hvalzs

end TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_LowerMedian_FallbackNecessary
