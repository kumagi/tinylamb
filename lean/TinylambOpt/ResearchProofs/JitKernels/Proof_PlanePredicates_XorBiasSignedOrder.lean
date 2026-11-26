import TinylambOpt.ResearchGoals.JitKernels.PlanePredicates

/-!
Flipping the sign bit of a two's-complement lane shifts its signed value up by
`2^31`, turning signed order into unsigned order. This lets one unsigned plane
comparator order signed lanes without a separate signed predicate.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace Nov26.Proof_PlanePredicates_XorBiasSignedOrder
open TinylambOpt.ResearchGoals.JitKernels.PlanePredicates

private theorem testBit_one_pos (k : Nat) (hk : 0 < k) : (1 : Nat).testBit k = false := by
  apply Nat.testBit_lt_two_pow
  exact Nat.one_lt_two_pow (by omega)

private theorem xor_top_of_lt (n : Nat) (hn : n < 2^31) : n ^^^ 2^31 = n + 2^31 := by
  apply Nat.eq_of_testBit_eq
  intro j
  rw [Nat.testBit_xor, Nat.testBit_two_pow]
  rw [show n + 2^31 = 2^31 * 1 + n by omega]
  rw [Nat.testBit_two_pow_mul_add 1 hn]
  by_cases hj : j < 31
  · have hne : 31 ≠ j := by omega
    simp [hj, hne]
  · by_cases hj31 : j = 31
    · subst j
      have hb : n.testBit 31 = false := Nat.testBit_lt_two_pow hn
      simp [hb]
    · have hjgt : 31 < j := by omega
      have hb : n.testBit j = false := Nat.testBit_lt_two_pow (by
        calc n < 2^31 := hn
          _ < 2^j := Nat.pow_lt_pow_right (by decide : 1 < 2) hjgt)
      rw [hb]
      rw [testBit_one_pos (j - 31) (by omega)]
      simp [show 31 ≠ j by omega]

private theorem xor_top_of_ge (n : Nat) (hlo : 2^31 ≤ n) (hhi : n < 2^32) :
    n ^^^ 2^31 = n - 2^31 := by
  have hr : n - 2^31 < 2^31 := by omega
  have hdec : n = 2^31 * 1 + (n - 2^31) := by omega
  apply Nat.eq_of_testBit_eq
  intro j
  rw [Nat.testBit_xor, Nat.testBit_two_pow, hdec, Nat.testBit_two_pow_mul_add 1 hr]
  by_cases hj : j < 31
  · have hne : 31 ≠ j := by omega
    simp [hj, hne]
  · by_cases hj31 : j = 31
    · subst j
      simp [Nat.testBit_lt_two_pow hr]
    · have hjgt : 31 < j := by omega
      have hb : (n - 2^31).testBit j = false := Nat.testBit_lt_two_pow (by
        calc n - 2^31 < 2^31 := hr
          _ < 2^j := Nat.pow_lt_pow_right (by decide : 1 < 2) hjgt)
      simp [hb, show j ≠ 31 by omega, show 31 ≠ j by omega, testBit_one_pos (j - 31) (by omega)]

theorem sound : XorBiasSignedOrder.Goal := by
  intro a b
  have hflip : ∀ v : BitVec 32, ((v ^^^ (0x80000000 : BitVec 32)).toNat : Int) = v.toInt + 2^31 := by
    intro v
    have hvnat : v.toNat < 2^32 := v.isLt
    have hconst : (0x80000000 : BitVec 32).toNat = 2^31 := by
      native_decide
    rw [BitVec.toNat_xor, hconst]
    rw [BitVec.toInt]
    by_cases h : 2 * v.toNat < 2^32
    · have hlt : v.toNat < 2^31 := by omega
      rw [xor_top_of_lt v.toNat hlt]
      simp [h]
    · have hge : 2^31 ≤ v.toNat := by omega
      rw [xor_top_of_ge v.toNat hge hvnat]
      simp [h]
      omega
  constructor
  · intro hab
    have hlt : ((a ^^^ (0x80000000 : BitVec 32)).toNat : Int) <
        ((b ^^^ (0x80000000 : BitVec 32)).toNat : Int) := by
      rw [hflip a, hflip b]
      omega
    exact Int.ofNat_lt.mp hlt
  · intro hab
    have hlt : ((a ^^^ (0x80000000 : BitVec 32)).toNat : Int) <
        ((b ^^^ (0x80000000 : BitVec 32)).toNat : Int) := Int.ofNat_lt.mpr hab
    rw [hflip a, hflip b] at hlt
    omega

end Nov26.Proof_PlanePredicates_XorBiasSignedOrder
