import TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
import TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common_LowerMedian

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_LowerMedian_FastRetraction

open TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
open TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.CommonLowerMedian
open List

theorem sound : LowerMedian.FastRetraction.Goal := by
  intro xs x out hx hd
  by_cases hlen : xs.length == 1
  · have hn : xs.length = 1 := by simpa using hlen
    have hxs : xs = [x] := by
      cases xs with
      | nil => simp at hn
      | cons y ys =>
          simp at hn
          subst ys
          have hxy : x = y := by simpa using hx
          subst y
          rfl
    subst xs
    simp [dropOne, value] at hd ⊢
    exact hd
  · have hlenb : (xs.length == 1) = false := by simpa using hlen
    have hpos : 0 < xs.length := List.length_pos_of_mem hx
    let v := lowerMedian xs
    have hv : v = lowerMedian xs := rfl
    cases hrm : LowerMedian.model.remove xs.length (LowerMedian.model.result xs) x with
    | none =>
        rw [dropOne, hlenb, hrm] at hd
        exact absurd hd (by simp)
    | some w =>
        have hout : some w = out := by simpa [dropOne, hlenb, hrm] using hd
        have hrm' : (if xs.length % 2 == 0 && x > v then some v
            else if xs.length % 2 == 1 && x < v then some v else none) = some w := by
          simpa [LowerMedian.model, ← hv] using hrm
        have hlene : (xs.erase x).length = xs.length - 1 := List.length_erase_of_mem hx
        have hlen_ne_one : xs.length ≠ 1 := by
          intro h1
          apply hlen
          simp [h1]
        have hlen_ge_two : 2 ≤ xs.length := by omega
        have hne : xs.erase x ≠ [] := by
          intro h
          have hl := congrArg List.length h
          rw [hlene] at hl
          simp at hl
          omega
        obtain ⟨h1b, h2b⟩ := bounds hpos hv.symm
        have hmed : lowerMedian (xs.erase x) = w := by
          rcases Nat.mod_two_eq_zero_or_one xs.length with hmod | hmod
          · -- even population: x is strictly above the median; the slot is unchanged
            have hif : (if x > v then some v else none) = some w := by simpa [hmod] using hrm'
            by_cases hxg : x > v
            · rw [if_pos hxg] at hif
              have hrew : w = v := (Option.some.inj hif).symm
              have hnotlt : (fun z => decide (z < v)) x = false := by
                simp [Nat.not_lt.mpr (Nat.le_of_lt hxg)]
              have hnotle : (fun z => decide (z ≤ v)) x = false := by
                simp [Nat.not_le.mpr hxg]
              have hc1base := countP_erase_of_mem (P := fun z => decide (z < v)) hx
              have hc2base := countP_erase_of_mem (P := fun z => decide (z ≤ v)) hx
              simp [hnotlt] at hc1base
              simp [hnotle] at hc2base
              have hltl : ((xs.erase x).length - 1) / 2 < (xs.erase x).length := by
                rw [hlene]
                omega
              have hm := lowerMedian_eq_of_bounds (xs := xs.erase x) (m := v)
                (t := ((xs.erase x).length - 1) / 2) rfl hltl
              have htE : ((xs.erase x).length - 1) / 2 = (xs.length - 1) / 2 := by
                rw [hlene]
                omega
              have hb1 : (xs.erase x).countP (fun z => decide (z < v)) ≤
                  ((xs.erase x).length - 1) / 2 := by
                rw [← hc1base, htE]
                exact h1b
              have hb2 : ((xs.erase x).length - 1) / 2 + 1 ≤
                  (xs.erase x).countP (fun z => decide (z ≤ v)) := by
                rw [← hc2base, htE]
                exact h2b
              exact (hm hb1 hb2).trans hrew.symm
            · rw [if_neg hxg] at hif
              exact absurd hif (by simp)
          · -- odd population: x is strictly below the median; the slot keeps its value
            have hif : (if x < v then some v else none) = some w := by simpa [hmod] using hrm'
            by_cases hxl : x < v
            · rw [if_pos hxl] at hif
              have hrew : w = v := (Option.some.inj hif).symm
              have hlt : (fun z => decide (z < v)) x = true := by simp [hxl]
              have hle : (fun z => decide (z ≤ v)) x = true := by simp [Nat.le_of_lt hxl]
              have hc1base := countP_erase_of_mem (P := fun z => decide (z < v)) hx
              have hc2base := countP_erase_of_mem (P := fun z => decide (z ≤ v)) hx
              simp [hlt] at hc1base
              simp [hle] at hc2base
              have hltl : ((xs.erase x).length - 1) / 2 < (xs.erase x).length := by
                rw [hlene]
                omega
              have htE : ((xs.erase x).length - 1) / 2 + 1 = (xs.length - 1) / 2 := by
                rw [hlene]
                omega
              have hb1 : (xs.erase x).countP (fun z => decide (z < v)) ≤
                  ((xs.erase x).length - 1) / 2 := by omega
              have hb2 : ((xs.erase x).length - 1) / 2 + 1 ≤
                  (xs.erase x).countP (fun z => decide (z ≤ v)) := by omega
              have hm := lowerMedian_eq_of_bounds (xs := xs.erase x) (m := v)
                (t := ((xs.erase x).length - 1) / 2) rfl hltl
              exact (hm hb1 hb2).trans hrew.symm
            · rw [if_neg hxl] at hif
              exact absurd hif (by simp)
        simpa [value, LowerMedian.model, List.isEmpty_eq_false_iff.mpr hne, hmed] using hout

end TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_LowerMedian_FastRetraction
