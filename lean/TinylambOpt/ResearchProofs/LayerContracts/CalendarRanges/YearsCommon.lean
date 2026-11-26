import TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false

/-! Elementary Gregorian arithmetic shared by the calendar-bucket models. -/

namespace TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges

open TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges

theorem years400 (y k : Nat) : years (y + 400*k) = years y + 146097*k := by
  have h4a : (100*k)*4 = 400*k := by rw [Nat.mul_assoc, Nat.mul_comm k 4, ← Nat.mul_assoc]
  have h100a : (4*k)*100 = 400*k := by rw [Nat.mul_assoc, Nat.mul_comm k 100, ← Nat.mul_assoc]
  have h400a : (1*k)*400 = 400*k := by rw [Nat.mul_assoc, Nat.mul_comm k 400, ← Nat.mul_assoc]
  have h4 : (y + 400*k)/4 = y/4 + 100*k := by
    rw [← h4a]
    exact Nat.add_mul_div_right y (100*k) (by omega : 0 < 4)
  have h100 : (y + 400*k)/100 = y/100 + 4*k := by
    rw [← h100a]
    exact Nat.add_mul_div_right y (4*k) (by omega : 0 < 100)
  have h400 : (y + 400*k)/400 = y/400 + 1*k := by
    rw [← h400a]
    exact Nat.add_mul_div_right y (1*k) (by omega : 0 < 400)
  unfold years
  rw [h4, h100, h400]
  have hm : 365 * (y + 400*k) = 365*y + 146000*k := by
    rw [Nat.mul_add]
    rw [← Nat.mul_assoc]
  rw [hm]
  omega

theorem years401 (y : Nat) : years (y+401) = years (y+1) + 146097 := by
  have h := years400 (y+1) 1
  simpa [Nat.add_assoc] using h

theorem years_diff_bounds (y : Nat) : 365 ≤ years (y+1) - years y ∧ years (y+1) - years y ≤ 366 := by
  unfold years
  omega

theorem monthStart_of (y m : Nat) (hm : m < 12) :
    monthStart (12*y+m) = years y + [0,31,59,90,120,151,181,212,243,273,304,334][m]! +
      (if 2 ≤ m && years (y+1) - years y == 366 then 1 else 0) := by
  unfold monthStart
  have hdiv : (12*y+m)/12 = y := by
    rw [Nat.add_comm (12*y) m, Nat.add_mul_div_left m y (by omega : 0 < 12)]
    simp [Nat.div_eq_of_lt hm]
  have hmod : (12*y+m)%12 = m := by
    rw [Nat.add_comm (12*y) m, Nat.add_mul_mod_self_left]
    exact Nat.mod_eq_of_lt hm
  rw [hdiv, hmod]

theorem monthStart_succ (n : Nat) : monthStart n < monthStart (n+1) := by
  let y := n / 12
  let m := n % 12
  have hm : m < 12 := by dsimp [m]; exact Nat.mod_lt n (by omega : 0 < 12)
  have hn : n = 12*y + m := by
    dsimp [y, m]
    exact (Nat.div_add_mod n 12).symm
  have hcases : m = 0 ∨ m = 1 ∨ m = 2 ∨ m = 3 ∨ m = 4 ∨ m = 5 ∨
      m = 6 ∨ m = 7 ∨ m = 8 ∨ m = 9 ∨ m = 10 ∨ m = 11 := by omega
  have hd := years_diff_bounds y
  rcases hcases with h0 | h1 | h2 | h3 | h4 | h5 | h6 | h7 | h8 | h9 | h10 | h11
  · rw [hn]
    rw [h0]
    rw [show 12*y+0 + 1 = 12*y+1 by omega]
    rw [monthStart_of y 0 (by omega), monthStart_of y 1 (by omega)]
    by_cases hleap : years (y+1) - years y == 366 <;> simp [hleap] <;> omega
  · rw [hn]
    rw [h1]
    rw [show 12*y+1 + 1 = 12*y+2 by omega]
    rw [monthStart_of y 1 (by omega), monthStart_of y 2 (by omega)]
    by_cases hleap : years (y+1) - years y == 366 <;> simp [hleap] <;> omega
  · rw [hn]
    rw [h2]
    rw [show 12*y+2 + 1 = 12*y+3 by omega]
    rw [monthStart_of y 2 (by omega), monthStart_of y 3 (by omega)]
    by_cases hleap : years (y+1) - years y == 366 <;> simp [hleap] <;> omega
  · rw [hn]
    rw [h3]
    rw [show 12*y+3 + 1 = 12*y+4 by omega]
    rw [monthStart_of y 3 (by omega), monthStart_of y 4 (by omega)]
    by_cases hleap : years (y+1) - years y == 366 <;> simp [hleap] <;> omega
  · rw [hn]
    rw [h4]
    rw [show 12*y+4 + 1 = 12*y+5 by omega]
    rw [monthStart_of y 4 (by omega), monthStart_of y 5 (by omega)]
    by_cases hleap : years (y+1) - years y == 366 <;> simp [hleap] <;> omega
  · rw [hn]
    rw [h5]
    rw [show 12*y+5 + 1 = 12*y+6 by omega]
    rw [monthStart_of y 5 (by omega), monthStart_of y 6 (by omega)]
    by_cases hleap : years (y+1) - years y == 366 <;> simp [hleap] <;> omega
  · rw [hn]
    rw [h6]
    rw [show 12*y+6 + 1 = 12*y+7 by omega]
    rw [monthStart_of y 6 (by omega), monthStart_of y 7 (by omega)]
    by_cases hleap : years (y+1) - years y == 366 <;> simp [hleap] <;> omega
  · rw [hn]
    rw [h7]
    rw [show 12*y+7 + 1 = 12*y+8 by omega]
    rw [monthStart_of y 7 (by omega), monthStart_of y 8 (by omega)]
    by_cases hleap : years (y+1) - years y == 366 <;> simp [hleap] <;> omega
  · rw [hn]
    rw [h8]
    rw [show 12*y+8 + 1 = 12*y+9 by omega]
    rw [monthStart_of y 8 (by omega), monthStart_of y 9 (by omega)]
    by_cases hleap : years (y+1) - years y == 366 <;> simp [hleap] <;> omega
  · rw [hn]
    rw [h9]
    rw [show 12*y+9 + 1 = 12*y+10 by omega]
    rw [monthStart_of y 9 (by omega), monthStart_of y 10 (by omega)]
    by_cases hleap : years (y+1) - years y == 366 <;> simp [hleap] <;> omega
  · rw [hn]
    rw [h10]
    rw [show 12*y+10 + 1 = 12*y+11 by omega]
    rw [monthStart_of y 10 (by omega), monthStart_of y 11 (by omega)]
    by_cases hleap : years (y+1) - years y == 366 <;> simp [hleap] <;> omega
  · rw [hn]
    rw [h11]
    rw [show 12*y+11 + 1 = 12*(y+1)+0 by omega]
    rw [monthStart_of y 11 (by omega), monthStart_of (y+1) 0 (by omega)]
    by_cases hleap : years (y+1) - years y == 366 <;> simp [hleap] <;> omega

theorem monthStart_add_4800 (n : Nat) : monthStart (n+4800) = monthStart n + 146097 := by
  let y := n / 12
  let m := n % 12
  have hm : m < 12 := by dsimp [m]; exact Nat.mod_lt n (by omega : 0 < 12)
  have hn : n = 12*y + m := by
    dsimp [y, m]
    exact (Nat.div_add_mod n 12).symm
  rw [hn]
  have hidx : 12*y + m + 4800 = 12*(y+400) + m := by omega
  rw [hidx]
  rw [monthStart_of (y+400) m hm, monthStart_of y m hm]
  have hy := years400 y 1
  have hy1 := years401 y
  have hdiff : years (y+400+1) - years (y+400) = years (y+1) - years y := by
    have h1 : y + 400 + 1 = y + 401 := by omega
    rw [h1, hy1, hy]
    omega
  have hleap : (years (y+400+1) - years (y+400) == 366) = (years (y+1) - years y == 366) := by
    rw [hdiff]
  have hy' : years (y+400) = years y + 146097 := by simpa using (years400 y 1)
  rw [hleap, hy']
  omega

theorem monthStart_zero : monthStart 0 = 0 := by
  unfold monthStart years
  simp


theorem years_strict (a b : Nat) (h : a < b) : years a < years b := by
  induction b generalizing a with
  | zero => omega
  | succ b ih =>
      by_cases ha : a < b
      · exact Nat.lt_trans (ih a ha) (by
          have hd := years_diff_bounds b
          omega)
      · have : a = b := by omega
        subst a
        have hd := years_diff_bounds b
        omega

theorem monthStart_mono (a b : Nat) (h : a ≤ b) : monthStart a ≤ monthStart b := by
  induction b generalizing a with
  | zero =>
      have : a = 0 := by omega
      subst a
      omega
  | succ b ih =>
      by_cases ha : a ≤ b
      · exact Nat.le_trans (ih a ha) (Nat.le_of_lt (monthStart_succ b))
      · have : a = b + 1 := by omega
        subst a
        omega

theorem monthStart_strict (a b : Nat) (h : a < b) : monthStart a < monthStart b := by
  have hle := monthStart_mono a (b-1) (by omega)
  have hs := monthStart_succ (b-1)
  have hidx : b - 1 + 1 = b := by omega
  rw [hidx] at hs
  omega

theorem monthStart_gap (n : Nat) : monthStart n + 16 ≤ monthStart (n+1) := by
  let y := n / 12
  let m := n % 12
  have hm : m < 12 := by dsimp [m]; exact Nat.mod_lt n (by omega : 0 < 12)
  have hn : n = 12*y + m := by
    dsimp [y, m]
    exact (Nat.div_add_mod n 12).symm
  have hcases : m = 0 ∨ m = 1 ∨ m = 2 ∨ m = 3 ∨ m = 4 ∨ m = 5 ∨
      m = 6 ∨ m = 7 ∨ m = 8 ∨ m = 9 ∨ m = 10 ∨ m = 11 := by omega
  have hd := years_diff_bounds y
  rcases hcases with h0 | h1 | h2 | h3 | h4 | h5 | h6 | h7 | h8 | h9 | h10 | h11
  · rw [hn]
    rw [h0]
    rw [show 12*y+0 + 1 = 12*y+1 by omega]
    rw [monthStart_of y 0 (by omega), monthStart_of y 1 (by omega)]
    by_cases hleap : years (y+1) - years y == 366 <;> simp [hleap] <;> omega
  · rw [hn]
    rw [h1]
    rw [show 12*y+1 + 1 = 12*y+2 by omega]
    rw [monthStart_of y 1 (by omega), monthStart_of y 2 (by omega)]
    by_cases hleap : years (y+1) - years y == 366 <;> simp [hleap] <;> omega
  · rw [hn]
    rw [h2]
    rw [show 12*y+2 + 1 = 12*y+3 by omega]
    rw [monthStart_of y 2 (by omega), monthStart_of y 3 (by omega)]
    by_cases hleap : years (y+1) - years y == 366 <;> simp [hleap] <;> omega
  · rw [hn]
    rw [h3]
    rw [show 12*y+3 + 1 = 12*y+4 by omega]
    rw [monthStart_of y 3 (by omega), monthStart_of y 4 (by omega)]
    by_cases hleap : years (y+1) - years y == 366 <;> simp [hleap] <;> omega
  · rw [hn]
    rw [h4]
    rw [show 12*y+4 + 1 = 12*y+5 by omega]
    rw [monthStart_of y 4 (by omega), monthStart_of y 5 (by omega)]
    by_cases hleap : years (y+1) - years y == 366 <;> simp [hleap] <;> omega
  · rw [hn]
    rw [h5]
    rw [show 12*y+5 + 1 = 12*y+6 by omega]
    rw [monthStart_of y 5 (by omega), monthStart_of y 6 (by omega)]
    by_cases hleap : years (y+1) - years y == 366 <;> simp [hleap] <;> omega
  · rw [hn]
    rw [h6]
    rw [show 12*y+6 + 1 = 12*y+7 by omega]
    rw [monthStart_of y 6 (by omega), monthStart_of y 7 (by omega)]
    by_cases hleap : years (y+1) - years y == 366 <;> simp [hleap] <;> omega
  · rw [hn]
    rw [h7]
    rw [show 12*y+7 + 1 = 12*y+8 by omega]
    rw [monthStart_of y 7 (by omega), monthStart_of y 8 (by omega)]
    by_cases hleap : years (y+1) - years y == 366 <;> simp [hleap] <;> omega
  · rw [hn]
    rw [h8]
    rw [show 12*y+8 + 1 = 12*y+9 by omega]
    rw [monthStart_of y 8 (by omega), monthStart_of y 9 (by omega)]
    by_cases hleap : years (y+1) - years y == 366 <;> simp [hleap] <;> omega
  · rw [hn]
    rw [h9]
    rw [show 12*y+9 + 1 = 12*y+10 by omega]
    rw [monthStart_of y 9 (by omega), monthStart_of y 10 (by omega)]
    by_cases hleap : years (y+1) - years y == 366 <;> simp [hleap] <;> omega
  · rw [hn]
    rw [h10]
    rw [show 12*y+10 + 1 = 12*y+11 by omega]
    rw [monthStart_of y 10 (by omega), monthStart_of y 11 (by omega)]
    by_cases hleap : years (y+1) - years y == 366 <;> simp [hleap] <;> omega
  · rw [hn]
    rw [h11]
    rw [show 12*y+11 + 1 = 12*(y+1)+0 by omega]
    rw [monthStart_of y 11 (by omega), monthStart_of (y+1) 0 (by omega)]
    by_cases hleap : years (y+1) - years y == 366 <;> simp [hleap] <;> omega

theorem monthStart3 : monthStart 3 = 90 := by
  rw [show 3 = 12*0+3 by omega]
  rw [monthStart_of 0 3 (by omega)]
  unfold years
  simp

theorem fiscalStart (y : Nat) :
    monthStart (12*y+3) - monthStart 3 = years y + (if years (y+1) - years y == 366 then 1 else 0) := by
  rw [monthStart_of y 3 (by omega), monthStart3]
  by_cases hleap : years (y+1) - years y == 366
  · simp [hleap]
  · simp [hleap]

theorem years_diff_eq (y : Nat) : years (y+1) - years y = 365 + (if years (y+1) - years y == 366 then 1 else 0) := by
  have hd := years_diff_bounds y
  by_cases hleap : years (y+1) - years y == 366
  · have h := beq_iff_eq.mp hleap
    simp [hleap]
    omega
  · have hne : years (y+1) - years y ≠ 366 := by intro h; exact hleap (beq_iff_eq.mpr h)
    simp [hleap]
    omega

end TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges
