import TinylambOpt.ResearchGoals.JitFrontiers.WordPromises

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.WordPromises.Goal_SignedProduct_GuardExact

open TinylambOpt.ResearchGoals.JitFrontiers.WordPromises
open TinylambOpt.ResearchGoals.JitFrontiers.WordPromises.SignedProduct

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


theorem sound : SignedProduct.GuardExact.Goal := by
  unfold SignedProduct.GuardExact.Goal guardExact
  dsimp [SignedProduct.op]
  intro x y
  rw [signed_not_overflow]
  change ((x * y).toInt == x.toInt * y.toInt) = (!(BitVec.smulOverflow x y))
  by_cases h : BitVec.smulOverflow x y = true
  · have hne : (x * y).toInt ≠ x.toInt * y.toInt := by
      intro heq
      have hz1 := BitVec.le_two_mul_toInt (x := x * y)
      have hz2 := BitVec.two_mul_toInt_lt (x := x * y)
      simp [BitVec.smulOverflow] at h
      omega
    have hne' : (x.toInt * y.toInt).bmod (2^64) ≠ x.toInt * y.toInt := by
      rw [← BitVec.toInt_mul]
      exact hne
    simp [h, hne']
  · have heq := BitVec.toInt_mul_of_not_smulOverflow h
    simp [h, heq]

end TinylambOpt.ResearchProofs.JitFrontiers.WordPromises.Goal_SignedProduct_GuardExact
