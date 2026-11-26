import TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Common_CalendarRanges
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.YearsCommon
import TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Facts_Decade

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Goal_Decade_DecadeDays

open TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges
open TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges.Decade
open TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges
open TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Facts_Decade

theorem sound : Decade.DecadeDays.Goal := by
  intro i
  let q := i / 40
  let k := i % 40
  have hk : k < 40 := by dsimp [k]; exact Nat.mod_lt i (by omega : 0 < 40)
  have hi : i = 40*q + k := by
    dsimp [q, k]
    simpa [Nat.mul_comm] using (Nat.div_add_mod i 40).symm
  have hkcase : k = 0 ∨ k = 1 ∨ k = 2 ∨ k = 3 ∨ k = 4 ∨ k = 5 ∨ k = 6 ∨ k = 7 ∨
      k = 8 ∨ k = 9 ∨ k = 10 ∨ k = 11 ∨ k = 12 ∨ k = 13 ∨ k = 14 ∨ k = 15 ∨
      k = 16 ∨ k = 17 ∨ k = 18 ∨ k = 19 ∨ k = 20 ∨ k = 21 ∨ k = 22 ∨ k = 23 ∨
      k = 24 ∨ k = 25 ∨ k = 26 ∨ k = 27 ∨ k = 28 ∨ k = 29 ∨ k = 30 ∨ k = 31 ∨
      k = 32 ∨ k = 33 ∨ k = 34 ∨ k = 35 ∨ k = 36 ∨ k = 37 ∨ k = 38 ∨ k = 39 := by omega
  have hspan : span Decade.model i = span Decade.model k := by
    unfold Decade.model span
    change years (10*(i+1)) - years (10*i) = years (10*(k+1)) - years (10*k)
    have h10i : 10*i = 400*q + 10*k := by dsimp [q, k] at *; omega
    have h10i1 : 10*(i+1) = 400*q + (10*k+10) := by omega
    rw [h10i, h10i1]
    have hA := years400 (10*k) q
    have hB := years400 (10*k+10) q
    have hA' : years (400*q+10*k) = years (10*k) + 146097*q := by
      rw [Nat.add_comm]
      exact hA
    have hB' : years (400*q+(10*k+10)) = years (10*k+10) + 146097*q := by
      rw [Nat.add_comm]
      exact hB
    rw [hA', hB']
    rw [show 10*(k+1) = 10*k+10 by omega]
    rw [Nat.add_sub_add_right]
  rw [hspan]
  rcases hkcase with hk0 | hk1 | hk2 | hk3 | hk4 | hk5 | hk6 | hk7 | hk8 | hk9 | hk10 | hk11 | hk12 | hk13 | hk14 | hk15 | hk16 | hk17 | hk18 | hk19 | hk20 | hk21 | hk22 | hk23 | hk24 | hk25 | hk26 | hk27 | hk28 | hk29 | hk30 | hk31 | hk32 | hk33 | hk34 | hk35 | hk36 | hk37 | hk38 | hk39
  · rw [hk0]
    native_decide
  · rw [hk1]
    native_decide
  · rw [hk2]
    native_decide
  · rw [hk3]
    native_decide
  · rw [hk4]
    native_decide
  · rw [hk5]
    native_decide
  · rw [hk6]
    native_decide
  · rw [hk7]
    native_decide
  · rw [hk8]
    native_decide
  · rw [hk9]
    native_decide
  · rw [hk10]
    native_decide
  · rw [hk11]
    native_decide
  · rw [hk12]
    native_decide
  · rw [hk13]
    native_decide
  · rw [hk14]
    native_decide
  · rw [hk15]
    native_decide
  · rw [hk16]
    native_decide
  · rw [hk17]
    native_decide
  · rw [hk18]
    native_decide
  · rw [hk19]
    native_decide
  · rw [hk20]
    native_decide
  · rw [hk21]
    native_decide
  · rw [hk22]
    native_decide
  · rw [hk23]
    native_decide
  · rw [hk24]
    native_decide
  · rw [hk25]
    native_decide
  · rw [hk26]
    native_decide
  · rw [hk27]
    native_decide
  · rw [hk28]
    native_decide
  · rw [hk29]
    native_decide
  · rw [hk30]
    native_decide
  · rw [hk31]
    native_decide
  · rw [hk32]
    native_decide
  · rw [hk33]
    native_decide
  · rw [hk34]
    native_decide
  · rw [hk35]
    native_decide
  · rw [hk36]
    native_decide
  · rw [hk37]
    native_decide
  · rw [hk38]
    native_decide
  · rw [hk39]
    native_decide


end TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges.Goal_Decade_DecadeDays
