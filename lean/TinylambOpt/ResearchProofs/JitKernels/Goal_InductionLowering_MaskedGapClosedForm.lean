import TinylambOpt.ResearchGoals.JitKernels.InductionLowering

/-!
Skipping `k` masked lanes is equivalent to one triangular-number update of the
quadratic recurrence state: the derivative grows by `k * dd` and the value by
`k * d + (k*(k-1)/2) * dd`. This closed form lets SQL execution jump over whole
gaps of a selection vector instead of iterating skipped rows.
-/

set_option maxHeartbeats 0
set_option linter.unusedSimpArgs false
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.InductionLowering.Goal_MaskedGapClosedForm

open TinylambOpt.ResearchGoals.JitKernels.InductionLowering

theorem mul_pred_add (k : Nat) : k * (k - 1) + k = k * k := by
  cases k with
  | zero => simp
  | succ j =>
      simp [Nat.mul_add, Nat.add_mul, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm]

theorem tri (k : Nat) : (k + 1) * k / 2 = k * (k - 1) / 2 + k := by
  have hd1 : 2 ∣ k * (k - 1) := by
    rcases Nat.mod_two_eq_zero_or_one k with hk | hk
    · have hdiv : 2 ∣ k := Nat.dvd_of_mod_eq_zero hk
      rcases hdiv with ⟨c, hc⟩
      exact ⟨c * (k - 1), by rw [hc]; ac_rfl⟩
    · have hdiv : 2 ∣ k - 1 := by
        apply Nat.dvd_of_mod_eq_zero
        omega
      rcases hdiv with ⟨c, hc⟩
      exact ⟨k * c, by rw [hc]; ac_rfl⟩
  have hd2 : 2 ∣ (k + 1) * k := by
    rcases Nat.mod_two_eq_zero_or_one k with hk | hk
    · have hdiv : 2 ∣ k := Nat.dvd_of_mod_eq_zero hk
      rcases hdiv with ⟨c, hc⟩
      exact ⟨(k + 1) * c, by rw [hc]; ac_rfl⟩
    · have hdiv : 2 ∣ k + 1 := by
        apply Nat.dvd_of_mod_eq_zero
        omega
      rcases hdiv with ⟨c, hc⟩
      exact ⟨c * k, by rw [hc]; ac_rfl⟩
  apply Nat.eq_of_mul_eq_mul_left (by decide : 0 < 2)
  rw [Nat.mul_div_cancel' hd2]
  rw [Nat.mul_add]
  rw [Nat.mul_div_cancel' hd1]
  have hmp := mul_pred_add k
  have h1 : (k + 1) * k = k * k + k := by simp [Nat.mul_add, Nat.mul_comm]
  omega

theorem foldl_closed (dd : Int) : ∀ k v d,
    (List.range k).foldl (fun s _ => advance dd s) (v, d) =
      (v + (k : Int) * d + (((k * (k - 1) / 2 : Nat) : Int)) * dd,
       d + (k : Int) * dd) := by
  intro k
  induction k with
  | zero =>
      intro v d
      simp
  | succ k ih =>
      intro v d
      rw [List.range_succ, List.foldl_append]
      rw [ih]
      simp only [List.foldl_cons, List.foldl_nil]
      unfold advance
      simp only [Int.natCast_add, Int.natCast_one]
      rw [show (k + 1) * (k + 1 - 1) / 2 = (k + 1) * k / 2 by simp]
      rw [tri k]
      simp only [Int.natCast_add, Int.natCast_mul, Int.natCast_one]
      apply Prod.ext <;> (simp only [Int.add_mul, Int.mul_add]; omega)

theorem sound : MaskedGapClosedForm.Goal := by
  intro v d dd k
  rw [foldl_closed dd k v d]

end TinylambOpt.ResearchProofs.JitKernels.InductionLowering.Goal_MaskedGapClosedForm
