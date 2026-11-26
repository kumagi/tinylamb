import TinylambOpt.ResearchGoals.JitKernels.InductionLowering

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_InductionLowering_MaskedGapClosedForm

set_option maxRecDepth 10000

theorem sound : TinylambOpt.ResearchGoals.JitKernels.InductionLowering.MaskedGapClosedForm.Goal := by
  intro v d dd k
  have htri : ∀ k : Nat, (k + 1) * k / 2 = k * (k - 1) / 2 + k := by
    intro k
    rcases Nat.mod_two_eq_zero_or_one k with hk | hk
    · have hkeq : k = 2 * (k / 2) := by
        have hdiv := Nat.div_add_mod k 2
        omega
      rw [hkeq]
      rw [show (2 * (k / 2) + 1) * (2 * (k / 2)) / 2 = (2 * (k / 2) + 1) * (k / 2) by
        rw [show (2 * (k / 2) + 1) * (2 * (k / 2)) = 2 * ((2 * (k / 2) + 1) * (k / 2)) by
          simp [Nat.mul_assoc, Nat.mul_comm]]
        rw [Nat.mul_div_right _ (by decide)]]
      rw [show (2 * (k / 2)) * (2 * (k / 2) - 1) / 2 = (k / 2) * (2 * (k / 2) - 1) by
        rw [show (2 * (k / 2)) * (2 * (k / 2) - 1) = 2 * ((k / 2) * (2 * (k / 2) - 1)) by
          simp [Nat.mul_assoc]]
        rw [Nat.mul_div_right _ (by decide)]]
      have heven : (2 * (k / 2) + 1) * (k / 2) = (k / 2) * (2 * (k / 2) - 1) + 2 * (k / 2) := by
        by_cases hm : k / 2 = 0
        · simp [hm]
        · have hsub : 2 * (k / 2) - 1 + 1 = 2 * (k / 2) := by omega
          have hmul : (k / 2) * (2 * (k / 2)) = (k / 2) * (2 * (k / 2) - 1) + (k / 2) := by
            rw [← hsub]
            rw [Nat.mul_add]
            simp
          calc
            (2 * (k / 2) + 1) * (k / 2) = (k / 2) * (2 * (k / 2)) + (k / 2) := by
              rw [Nat.mul_comm]
              rw [Nat.mul_add]
              simp
            _ = ((k / 2) * (2 * (k / 2) - 1) + (k / 2)) + (k / 2) := by rw [hmul]
            _ = (k / 2) * (2 * (k / 2) - 1) + 2 * (k / 2) := by omega
      exact heven
    · have hkeq : k = 2 * (k / 2) + 1 := by
        have hdiv := Nat.div_add_mod k 2
        omega
      rw [hkeq]
      rw [show (2 * (k / 2) + 1 + 1) * (2 * (k / 2) + 1) / 2 =
          (k / 2 + 1) * (2 * (k / 2) + 1) by
        rw [show 2 * (k / 2) + 1 + 1 = 2 * (k / 2 + 1) by omega]
        rw [show (2 * (k / 2 + 1)) * (2 * (k / 2) + 1) =
            2 * ((k / 2 + 1) * (2 * (k / 2) + 1)) by
          simp [Nat.mul_comm, Nat.mul_left_comm]]
        rw [Nat.mul_div_right _ (by decide)]]
      rw [show (2 * (k / 2) + 1) * (2 * (k / 2) + 1 - 1) / 2 =
          (2 * (k / 2) + 1) * (k / 2) by
        have hlin : 2 * (k / 2) + 1 - 1 = 2 * (k / 2) := by omega
        rw [hlin]
        rw [show (2 * (k / 2) + 1) * (2 * (k / 2)) =
            2 * ((2 * (k / 2) + 1) * (k / 2)) by
          simp [Nat.mul_assoc, Nat.mul_comm]]
        rw [Nat.mul_div_right _ (by decide)]]
      have hodd : (k / 2 + 1) * (2 * (k / 2) + 1) =
          (2 * (k / 2) + 1) * (k / 2) + (2 * (k / 2) + 1) := by
        rw [Nat.add_mul]
        simp [Nat.mul_comm]
      exact hodd
  have hstep : ∀ k : Nat,
      (List.range k).foldl (fun s _ => TinylambOpt.ResearchGoals.JitKernels.InductionLowering.advance dd s) (v, d) =
        (v + (k : Int) * d + ((k * (k - 1) / 2 : Nat) : Int) * dd,
         d + (k : Int) * dd) := by
    intro k
    induction k with
    | zero => simp
    | succ k ih =>
        rw [List.range_succ, List.foldl_append]
        simp only [List.foldl_cons, List.foldl_nil]
        rw [ih]
        unfold TinylambOpt.ResearchGoals.JitKernels.InductionLowering.advance
        simp only
        simp only [Nat.add_sub_cancel]
        rw [htri]
        simp
        constructor <;> simp [Int.mul_add, Int.mul_comm] <;> omega
  have hs := hstep k
  exact congrArg Prod.fst hs

end TinylambOpt.ResearchProofs.JitKernels.Goal_InductionLowering_MaskedGapClosedForm
