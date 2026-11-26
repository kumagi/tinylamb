import TinylambOpt.ResearchGoals.JitFrontiers.WordPromises

/-!
A fixed-width SQL expression fast path depends on the integer range and representation
facts used to justify its machine operations.

Proved

- `SignedNarrow.GuardExact` — A concrete machine-bit test recognizes precisely the
  defined domain.
- `SignedNarrow.KeyIdentity` — `∀ x y v, ir op (x,y)=some v → v=x`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.WordPromises.Goal_SignedNarrow_KeyIdentity

open TinylambOpt.ResearchGoals.JitFrontiers.WordPromises

private theorem hSN : SignedNarrow.GuardExact.Goal := by
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


theorem sound : SignedNarrow.KeyIdentity.Goal := by
  intro x y v h
  unfold ir at h
  dsimp [SignedNarrow.op] at h
  simp at h
  have hdef : SignedNarrow.op.defined x y = true := by
    dsimp [SignedNarrow.op]
    simp [h.1.1, h.1.2]
  have httest : SignedNarrow.op.test x y = true := by
    rw [hSN x y]
    exact hdef
  have ht : (signExt32 x == x) = true := by
    simpa [SignedNarrow.op] using httest
  have hx : signExt32 x = x := by simpa [beq_iff_eq] using ht
  rw [← h.2]
  exact hx

end TinylambOpt.ResearchProofs.JitFrontiers.WordPromises.Goal_SignedNarrow_KeyIdentity
