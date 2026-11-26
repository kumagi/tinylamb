import TinylambOpt.ResearchGoals.JitFrontiers.WordPromises

/-!
A fixed-width SQL expression fast path depends on the integer range and representation
facts used to justify its machine operations.

Proved

- `SignedNarrow.GuardExact` — A concrete machine-bit test recognizes precisely the
  defined domain.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.WordPromises.Goal_SignedNarrow_GuardExact

open TinylambOpt.ResearchGoals.JitFrontiers.WordPromises
open TinylambOpt.ResearchGoals.JitFrontiers.WordPromises.SignedNarrow

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


theorem sound : SignedNarrow.GuardExact.Goal := by
  unfold SignedNarrow.GuardExact.Goal guardExact
  dsimp [SignedNarrow.op]
  intro x y
  apply Bool.eq_iff_iff.mpr
  rw [beq_iff_eq]
  constructor
  · intro h
    have ht : (signExt32 x).toInt = x.toInt := congrArg BitVec.toInt h
    dsimp [signExt32] at ht
    simp only [BitVec.toInt_ofInt] at ht
    have hz1 := BitVec.le_two_mul_toInt (x := BitVec.ofNat 32 x.toNat)
    have hz2 := BitVec.two_mul_toInt_lt (x := BitVec.ofNat 32 x.toNat)
    have hmb : ((BitVec.ofNat 32 x.toNat).toInt).bmod (2^64) = (BitVec.ofNat 32 x.toNat).toInt := by
      apply Int.bmod_eq_of_le <;> omega
    rw [hmb] at ht
    simp only [Bool.and_eq_true, decide_eq_true_eq]
    constructor <;> omega
  · intro hrange
    simp only [Bool.and_eq_true, decide_eq_true_eq] at hrange
    have hlo : -(2^31 : Int) ≤ x.toInt := hrange.1
    have hhi : x.toInt < (2^31 : Int) := hrange.2
    have hdiv : (2^32 : Int) ∣ 2^64 := by decide
    have hxn : x.toInt % (2^64 : Int) = (x.toNat : Int) := BitVec.toInt_emod_two_pow x
    have hxm : x.toInt % (2^32 : Int) = (x.toNat : Int) % (2^32 : Int) := by
      rw [← Int.emod_emod_of_dvd x.toInt hdiv]
      rw [hxn]
    have hzm : (BitVec.ofNat 32 x.toNat).toInt % (2^32 : Int) =
        ((BitVec.ofNat 32 x.toNat).toNat : Int) := BitVec.toInt_emod_two_pow _
    have hzto : ((BitVec.ofNat 32 x.toNat).toNat : Int) = (x.toNat : Int) % (2^32 : Int) := by
      simp
    have hm : (BitVec.ofNat 32 x.toNat).toInt % (2^32 : Int) = (x.toNat : Int) % (2^32 : Int) := by
      rw [hzm, hzto]
    have hz1 := BitVec.le_two_mul_toInt (x := BitVec.ofNat 32 x.toNat)
    have hz2 := BitVec.two_mul_toInt_lt (x := BitVec.ofNat 32 x.toNat)
    have htInt : (BitVec.ofNat 32 x.toNat).toInt = x.toInt := by omega
    apply BitVec.toInt_inj.mp
    dsimp [signExt32]
    rw [BitVec.toInt_ofInt]
    have hmb : ((BitVec.ofNat 32 x.toNat).toInt).bmod (2^64) = (BitVec.ofNat 32 x.toNat).toInt := by
      apply Int.bmod_eq_of_le <;> omega
    rw [hmb]
    exact htInt

end TinylambOpt.ResearchProofs.JitFrontiers.WordPromises.Goal_SignedNarrow_GuardExact
