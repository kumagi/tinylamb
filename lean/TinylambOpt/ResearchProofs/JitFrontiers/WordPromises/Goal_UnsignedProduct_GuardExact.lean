import TinylambOpt.ResearchGoals.JitFrontiers.WordPromises

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.WordPromises.Goal_UnsignedProduct_GuardExact

open TinylambOpt.ResearchGoals.JitFrontiers.WordPromises
open TinylambOpt.ResearchGoals.JitFrontiers.WordPromises.UnsignedProduct

private theorem signed_not_overflow (n : Int) :
    signed n = !((n ≥ (2^63 : Int)) || (n < -(2^63 : Int))) := by
  unfold signed
  generalize hA : decide (-(2^63 : Int) ≤ n) = A
  generalize hB : decide (n < (2^63 : Int)) = B
  generalize hC : decide (n ≥ (2^63 : Int)) = C
  generalize hD : decide (n < -(2^63 : Int)) = D
  cases A <;> cases B <;> cases C <;> cases D <;> simp_all <;> omega

private theorem floorMask_toNat (y : W) (hy : y.toNat < 64) :
    (floorMask y).toNat = 2 ^ y.toNat - 1 := by
  have hpow : 2 ^ y.toNat < 2^64 := Nat.pow_lt_pow_right (by omega) hy
  have hpowpos : 1 ≤ 2 ^ y.toNat := Nat.one_le_two_pow
  unfold floorMask
  rw [BitVec.toNat_sub]
  change (2^64 - 1 + (↑(1 <<< y.toNat) : W).toNat) % 2^64 = _
  have hof : (↑(1 <<< y.toNat) : W).toNat = 2 ^ y.toNat := by
    rw [Nat.shiftLeft_eq]
    simp [Nat.mod_eq_of_lt hpow]
  rw [hof]
  rw [show 2^64 - 1 + 2 ^ y.toNat = 2^64 + (2 ^ y.toNat - 1) by omega]
  rw [Nat.add_mod_left, Nat.mod_eq_of_lt (by omega)]


theorem sound : UnsignedProduct.GuardExact.Goal := by
  unfold UnsignedProduct.GuardExact.Goal guardExact
  dsimp [UnsignedProduct.op]
  intro x y
  by_cases hyz : y == (0 : W)
  · have hy0 : y = 0 := by simpa using hyz
    subst y
    simp
  · have hyne : y ≠ 0 := by
      intro h
      subst h
      simp at hyz
    have hypos : 0 < y.toNat := by
      have hne : y.toNat ≠ 0 := by
        intro h
        apply hyne
        exact BitVec.eq_of_toNat_eq h
      omega
    have hiff : (x.toNat ≤ (2^64 - 1) / y.toNat) ↔ x.toNat * y.toNat < 2^64 := by
      rw [Nat.le_div_iff_mul_le hypos]
      omega
    simp [hiff]
    intro hy0
    subst y
    simp

end TinylambOpt.ResearchProofs.JitFrontiers.WordPromises.Goal_UnsignedProduct_GuardExact
