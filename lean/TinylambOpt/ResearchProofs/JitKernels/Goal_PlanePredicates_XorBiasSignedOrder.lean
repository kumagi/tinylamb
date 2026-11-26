import TinylambOpt.ResearchGoals.JitKernels.PlanePredicates

/-!
Flipping the sign bit of a two's-complement lane converts signed order into
unsigned order. The proof characterizes the effect of xor with `2^31` on a
32-bit value (add or subtract `2^31`, depending on the sign bit) by bitplane
extensionality, unfolds `BitVec.toInt`, and settles each of the four sign-bit
cases with integer arithmetic.

Proved

- `XorBiasSignedOrder` — an unsigned plane comparator can order signed lanes
  after the sign-bit flip, preserving SQL comparison results.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_PlanePredicates_XorBiasSignedOrder

open TinylambOpt.ResearchGoals.JitKernels.PlanePredicates

/-- XOR with the 32-bit sign bit adds `2^31` to a nonnegative lane and
subtracts it from a negative lane. -/
theorem flip_msb_toNat (x : Nat) (hx : x < 2^32) :
    x ^^^ 2^31 = if x < 2^31 then x + 2^31 else x - 2^31 := by
  by_cases h : x < 2^31
  · simp only [h, ite_true]
    apply Nat.eq_of_testBit_eq
    intro i
    by_cases hi : i = 31
    · subst i
      have hx31 : x.testBit 31 = false := Nat.testBit_lt_two_pow h
      have hp31 : (2^31).testBit 31 = true := by simpa using (Nat.testBit_two_pow_self (n := 31))
      have hs31 : (x + 2^31).testBit 31 = true := by
        rw [Nat.add_comm, Nat.testBit_two_pow_add_eq, hx31]
        rfl
      simp [Nat.testBit_xor, hx31, hp31, hs31]
    · by_cases hi_lt : i < 31
      · have hp : (2^31).testBit i = false := by
          have ht := Nat.testBit_two_pow (n := 31) (m := i)
          simpa [show ¬ 31 = i by omega] using ht
        have hs : (x + 2^31).testBit i = x.testBit i := by
          rw [Nat.add_comm, Nat.testBit_two_pow_add_gt hi_lt]
        simp [Nat.testBit_xor, hp, hs]
      · have hi_gt : 31 < i := by omega
        have hpow : 2^32 ≤ 2^i := Nat.pow_le_pow_right (by omega) (by omega)
        have hx_lt : x < 2^i := by omega
        have hs_lt : x + 2^31 < 2^i := by omega
        have hp_lt : 2^31 < 2^i := Nat.pow_lt_pow_right (by omega) (by omega)
        have hxbit : x.testBit i = false := Nat.testBit_lt_two_pow hx_lt
        have hpbit : (2^31).testBit i = false := Nat.testBit_lt_two_pow hp_lt
        have hsbit : (x + 2^31).testBit i = false := Nat.testBit_lt_two_pow hs_lt
        simp [Nat.testBit_xor, hxbit, hpbit, hsbit]
  · simp only [h, ite_false]
    apply Nat.eq_of_testBit_eq
    intro i
    have hle : 2^31 ≤ x := by omega
    have hx_eq : x = 2^31 + (x - 2^31) := by omega
    have hy_lt : x - 2^31 < 2^31 := by omega
    by_cases hi : i = 31
    · subst i
      have hx31 : x.testBit 31 = true :=
        Nat.testBit_of_two_pow_le_and_two_pow_add_one_gt hle (by simpa using hx)
      have hp31 : (2^31).testBit 31 = true := by simpa using (Nat.testBit_two_pow_self (n := 31))
      have hy31 : (x - 2^31).testBit 31 = false := Nat.testBit_lt_two_pow hy_lt
      simp [Nat.testBit_xor, hx31, hp31, hy31]
    · by_cases hi_lt : i < 31
      · have hp : (2^31).testBit i = false := by
          have ht := Nat.testBit_two_pow (n := 31) (m := i)
          simpa [show ¬ 31 = i by omega] using ht
        have hxbit : x.testBit i = (x - 2^31).testBit i := by
          have hstep : (2^31 + (x - 2^31)).testBit i = (x - 2^31).testBit i :=
            Nat.testBit_two_pow_add_gt hi_lt (x - 2^31)
          exact (congrArg (fun n => n.testBit i) hx_eq).trans hstep
        simp [Nat.testBit_xor, hxbit, hp]
      · have hi_gt : 31 < i := by omega
        have hpow : 2^32 ≤ 2^i := Nat.pow_le_pow_right (by omega) (by omega)
        have hx_lt : x < 2^i := by omega
        have hp_lt : 2^31 < 2^i := Nat.pow_lt_pow_right (by omega) (by omega)
        have hy_lt' : x - 2^31 < 2^i := by omega
        have hxbit : x.testBit i = false := Nat.testBit_lt_two_pow hx_lt
        have hpbit : (2^31).testBit i = false := Nat.testBit_lt_two_pow hp_lt
        have hybit : (x - 2^31).testBit i = false := Nat.testBit_lt_two_pow hy_lt'
        simp [Nat.testBit_xor, hxbit, hpbit, hybit]

theorem sound : XorBiasSignedOrder.Goal := by
  intro a b
  unfold BitVec.toInt
  rw [BitVec.toNat_xor, BitVec.toNat_xor]
  have hconst : ((0x80000000 : BitVec 32).toNat) = 2^31 := by native_decide
  rw [hconst]
  have ha_lt : a.toNat < 2^32 := BitVec.isLt a
  have hb_lt : b.toNat < 2^32 := BitVec.isLt b
  rw [flip_msb_toNat a.toNat ha_lt, flip_msb_toNat b.toNat hb_lt]
  have ha2 : (2 * a.toNat < 2^32) ↔ a.toNat < 2^31 := by omega
  have hb2 : (2 * b.toNat < 2^32) ↔ b.toNat < 2^31 := by omega
  simp only [ha2, hb2, ite_true, ite_false]
  by_cases ha : a.toNat < 2^31 <;> by_cases hb : b.toNat < 2^31 <;> simp [ha, hb] <;> omega

end TinylambOpt.ResearchProofs.JitKernels.Goal_PlanePredicates_XorBiasSignedOrder
