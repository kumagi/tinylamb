import TinylambOpt.ResearchGoals.JitKernels.PlanePredicates

/-!
A plane on which the threshold and the residual agree carries no ordering
information. Flipping that bit in both operands leaves every decision of the
least-to-most-significant scan unchanged, so the plane can be elided from the
generated ballot logic.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace Nov26.Proof_PlanePredicates_CommonPlaneElision
open TinylambOpt.ResearchGoals.JitKernels.PlanePredicates

private theorem foldBits_succ (x c w : Nat) :
    foldBits x c (w + 1) =
      (if bit x w == bit c w then foldBits x c w else bit c w) := by
  unfold foldBits
  rw [List.range_succ, List.foldl_append]
  rfl

private theorem bit_xor_other (x j t : Nat) (h : j ≠ t) :
    bit (x ^^^ 2^j) t = bit x t := by
  unfold bit
  rw [Nat.testBit_xor, Nat.testBit_two_pow]
  simp [h]

private theorem bit_xor_self (x j : Nat) :
    bit (x ^^^ 2^j) j = (!bit x j) := by
  unfold bit
  rw [Nat.testBit_xor, Nat.testBit_two_pow]
  cases x.testBit j <;> simp

private theorem foldBits_flip_above (x c j w : Nat) (h : w ≤ j) :
    foldBits (x ^^^ 2^j) (c ^^^ 2^j) w = foldBits x c w := by
  induction w with
  | zero => simp [foldBits]
  | succ w ih =>
      rw [foldBits_succ, foldBits_succ, ih (by omega)]
      rw [bit_xor_other x j w (by omega), bit_xor_other c j w (by omega)]

theorem sound : CommonPlaneElision.Goal := by
  intro x c j w hbit
  induction w generalizing x c with
  | zero => simp [foldBits]
  | succ k ih =>
      rw [foldBits_succ, foldBits_succ]
      by_cases hjk : j < k
      · rw [ih x c hbit]
        rw [bit_xor_other x j k (by omega), bit_xor_other c j k (by omega)]
      · by_cases hjeq : j = k
        · subst j
          rw [foldBits_flip_above x c k k (by omega)]
          rw [bit_xor_self x k, bit_xor_self c k]
          rw [hbit]
          simp
        · have hjgt : k < j := by omega
          rw [foldBits_flip_above x c j k (by omega)]
          rw [bit_xor_other x j k (by omega), bit_xor_other c j k (by omega)]

end Nov26.Proof_PlanePredicates_CommonPlaneElision
