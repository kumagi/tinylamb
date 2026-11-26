import TinylambOpt.ResearchGoals.JitFrontiers.WordPromises

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.WordPromises.Goal_UnsignedSub_GuardExact

open TinylambOpt.ResearchGoals.JitFrontiers.WordPromises
open TinylambOpt.ResearchGoals.JitFrontiers.WordPromises.UnsignedSub

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


theorem sound : UnsignedSub.GuardExact.Goal := by
  unfold UnsignedSub.GuardExact.Goal guardExact
  dsimp [UnsignedSub.op]
  intro x y
  by_cases h : y.toNat ≤ x.toNat
  · have hmod : (2^64 - y.toNat + x.toNat) % 2^64 = x.toNat - y.toNat := by
      rw [show 2^64 - y.toNat + x.toNat = 2^64 + (x.toNat - y.toNat) by omega]
      rw [Nat.add_mod_left, Nat.mod_eq_of_lt]
      omega
    simp [h, BitVec.le_def, BitVec.toNat_sub, hmod, show x.toNat - y.toNat ≤ x.toNat by omega]
  · have hlt : x.toNat < y.toNat := by omega
    have hmod : (2^64 - y.toNat + x.toNat) % 2^64 = 2^64 - y.toNat + x.toNat := by
      exact Nat.mod_eq_of_lt (by omega)
    have hle : ¬ 2^64 - y.toNat + x.toNat ≤ x.toNat := by omega
    simp [h, BitVec.le_def, BitVec.toNat_sub, hmod, hle]

end TinylambOpt.ResearchProofs.JitFrontiers.WordPromises.Goal_UnsignedSub_GuardExact
