import TinylambOpt.ResearchGoals.JitFrontiers.WordPromises

/-!
A fixed-width SQL expression fast path depends on the integer range and representation
facts used to justify its machine operations.

Proved

- `UnsignedProduct.GuardExact` — A concrete machine-bit test recognizes precisely the
  defined domain.
- `UnsignedProduct.PowerOfTwo` — `∀ x k, k<64 → (op.test x (1 <<< k)=true ↔
  x.toNat<2^(64-k))`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.WordPromises.Goal_UnsignedProduct_PowerOfTwo

open TinylambOpt.ResearchGoals.JitFrontiers.WordPromises

private theorem hUP : UnsignedProduct.GuardExact.Goal := by
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


theorem sound : UnsignedProduct.PowerOfTwo.Goal := by
  intro x k hk
  have htest : UnsignedProduct.op.test x (1 <<< k) = UnsignedProduct.op.defined x (1 <<< k) :=
    hUP x (1 <<< k)
  rw [htest]
  dsimp [UnsignedProduct.op]
  have hto : (BitVec.ofNat 64 (1 <<< k)).toNat = 2 ^ k := by
    rw [BitVec.toNat_ofNat, Nat.shiftLeft_eq]
    simpa [Nat.one_mul] using (Nat.mod_eq_of_lt (Nat.pow_lt_pow_right (by decide : 1 < 2) hk) : 2^k % 2^64 = 2^k)
  rw [hto]
  simp only [decide_eq_true_eq]
  change x.toNat * 2^k < 2^64 ↔ x.toNat < 2^(64-k)
  rw [show 2^64 = 2^k * 2^(64-k) by
    have hp : k + (64 - k) = 64 := by omega
    calc
      2^64 = 2 ^ (k + (64 - k)) := by rw [hp]
      _ = 2^k * 2^(64-k) := Nat.pow_add 2 k (64-k)]
  rw [Nat.mul_comm x.toNat (2 ^ k)]
  rw [Nat.mul_lt_mul_left (Nat.two_pow_pos k)]

end TinylambOpt.ResearchProofs.JitFrontiers.WordPromises.Goal_UnsignedProduct_PowerOfTwo
