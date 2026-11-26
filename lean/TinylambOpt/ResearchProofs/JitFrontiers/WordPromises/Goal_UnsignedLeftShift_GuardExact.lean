import TinylambOpt.ResearchGoals.JitFrontiers.WordPromises

/-!
A fixed-width SQL expression fast path depends on the integer range and representation
facts used to justify its machine operations.

Proved

- `UnsignedLeftShift.GuardExact` — A concrete machine-bit test recognizes precisely the
  defined domain.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.WordPromises.Goal_UnsignedLeftShift_GuardExact

open TinylambOpt.ResearchGoals.JitFrontiers.WordPromises
open TinylambOpt.ResearchGoals.JitFrontiers.WordPromises.UnsignedLeftShift

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


theorem sound : UnsignedLeftShift.GuardExact.Goal := by
  unfold UnsignedLeftShift.GuardExact.Goal guardExact
  dsimp [UnsignedLeftShift.op]
  intro x y
  by_cases hy : y.toNat < 64
  · have hround : (((x <<< y.toNat) >>> y.toNat) == x) = decide (x.toNat * 2 ^ y.toNat < 2^64) := by
      apply Bool.eq_iff_iff.mpr
      rw [beq_iff_eq, decide_eq_true_eq]
      constructor
      · intro h
        have hle : ((x.toNat * 2^y.toNat) % 2^64) / 2^y.toNat * 2^y.toNat ≤
            (x.toNat * 2^y.toNat) % 2^64 := Nat.div_mul_le_self _ _
        have hrto : ((x <<< y.toNat) >>> y.toNat).toNat =
            ((x.toNat * 2^y.toNat) % 2^64) / 2^y.toNat := by
          rw [BitVec.toNat_ushiftRight, BitVec.toNat_shiftLeft]
          simp only [Nat.shiftLeft_eq, Nat.shiftRight_eq_div_pow]
        have hxto : x.toNat = ((x.toNat * 2^y.toNat) % 2^64) / 2^y.toNat := by
          have := congrArg BitVec.toNat h
          rw [hrto] at this
          simpa using this.symm
        have hle' : x.toNat * 2^y.toNat ≤ (x.toNat * 2^y.toNat) % 2^64 := by
          rw [← hxto] at hle
          exact hle
        have hlt : (x.toNat * 2^y.toNat) % 2^64 < 2^64 := Nat.mod_lt _ (by decide)
        omega
      · intro hlt
        apply BitVec.eq_of_toNat_eq
        rw [BitVec.toNat_ushiftRight, BitVec.toNat_shiftLeft]
        simp only [Nat.shiftLeft_eq, Nat.shiftRight_eq_div_pow]
        rw [Nat.mod_eq_of_lt hlt]
        rw [Nat.mul_comm]
        exact Nat.mul_div_right x.toNat (Nat.two_pow_pos y.toNat)
    have hdec : decide (y.toNat < 64) = true := by simp [hy]
    rw [hdec, hround]
  · have hdec : decide (y.toNat < 64) = false := by simp [hy]
    rw [hdec]
    simp

end TinylambOpt.ResearchProofs.JitFrontiers.WordPromises.Goal_UnsignedLeftShift_GuardExact
