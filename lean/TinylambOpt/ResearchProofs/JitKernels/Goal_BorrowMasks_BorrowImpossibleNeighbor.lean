import TinylambOpt.ResearchGoals.JitKernels.BorrowMasks
import TinylambOpt.ResearchProofs.JitKernels.BorrowMasksLemmas

/-! When the input alphabet excludes byte one, a borrow cannot continue past a
    zero byte, so the subtract mask agrees with the exact zero-byte mask. SQL
    may omit per-byte candidate verification in that case. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_BorrowMasks_BorrowImpossibleNeighbor

open TinylambOpt.ResearchGoals.SwarDigitBallot
open TinylambOpt.ResearchGoals.JitKernels.BorrowMasks
open Nov26.BorrowMasksLemmas

private theorem high_lsb_false (j : Nat) (hj : j < 64) (hmod : j % 8 ≠ 7) :
    high.getLsbD j = false := by
  exact (show ∀ j : Fin 64, j.val % 8 ≠ 7 → high.getLsbD j.val = false from by
    native_decide) ⟨j, hj⟩ hmod

private theorem high_only_false (m : W) (honly : m &&& ~~~high = 0) (j : Nat)
    (hj : j < 64) (hmod : j % 8 ≠ 7) : m.getLsbD j = false := by
  cases hbit : m.getLsbD j
  · rfl
  · exfalso
    have hm : m.getLsbD j = true := hbit
    have hhigh : high.getLsbD j = false := high_lsb_false j hj hmod
    have hn : (~~~high).getLsbD j = true := by simpa [hhigh, hj]
    have ht : (m &&& ~~~high).getLsbD j = true := by
      simp [BitVec.getLsbD_and, hm, hn]
    have hne : (m &&& ~~~high) ≠ 0 := by
      intro heq
      rw [heq] at ht
      simp at ht
    exact hne honly

private theorem zeroBytes_lsb_lane (y : W) (k : Nat) (hk : k < 8) :
    (zeroBytes y).getLsbD (8 * k + 7) = decide (lane y k = 0) := by
  rw [zeroBytes_lsb y k hk]
  unfold lane
  apply Bool.eq_iff_iff.mpr
  constructor
  · intro h
    simp at h ⊢
    have ht := congrArg BitVec.toNat h
    simpa [BitVec.extractLsb'_toNat] using ht
  · intro h
    simp at h ⊢
    exact BitVec.eq_of_toNat_eq h

private theorem cases8 (i : Nat) (hi : i < 8) :
    i = 0 ∨ i = 1 ∨ i = 2 ∨ i = 3 ∨ i = 4 ∨ i = 5 ∨ i = 6 ∨ i = 7 := by
  omega

private def lowBytes : Nat → Nat → Nat
  | 0, _ => 0
  | k + 1, n => lowBytes k n + 256 ^ k * (n / 256 ^ k % 256)

private theorem mod_lowBytes (k n : Nat) : n % 256 ^ k = lowBytes k n := by
  induction k with
  | zero => simp [lowBytes]; omega
  | succ k ih =>
      rw [Nat.mod_pow_succ, ih]
      rfl

private theorem mod_highByte (k n : Nat) :
    n % (128 * 256 ^ k) = lowBytes k n + 256 ^ k * (n / 256 ^ k % 128) := by
  rw [Nat.mul_comm 128 (256 ^ k), Nat.mod_mul, mod_lowBytes]

private theorem lane_ne_zero_of_high (x : W) (k : Nat) (hk : k < 8)
    (hhi : x.getLsbD (8 * k + 7) = true) : lane x k ≠ 0 := by
  rcases cases8 k hk with rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl <;>
    (have ht := hhi
     simp [BitVec.getLsbD, Nat.testBit_eq_decide_div_mod_eq] at ht
     unfold lane
     simp [BitVec.extractLsb'_toNat, Nat.shiftRight_eq_div_pow]
     omega)

private theorem fast_high_lsb (x : W) (k : Nat) (hk : k < 8)
    (hne : lane x k ≠ 1) (hhi : x.getLsbD (8 * k + 7) = false) :
    (fast x).getLsbD (8 * k + 7) = decide (lane x k = 0) := by
  rcases cases8 k hk with rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl
  ·
    change (fast x).getLsbD 7 = decide (x.toNat % 256 = 0)
    have ht := hhi
    simp [BitVec.getLsbD, Nat.testBit_eq_decide_div_mod_eq] at ht
    have hlt : x.toNat % 256 < 128 := by omega
    have hm : x.toNat % 128 = x.toNat % 256 := by
      rw [← Nat.mod_mod_of_dvd x.toNat (show 128 ∣ 256 by decide)]
      exact Nat.mod_eq_of_lt hlt
    simp [fast, ones, high, ← BitVec.getLsbD_eq_getElem, BitVec.getLsbD_sub,
      BitVec.carry, hhi]
    rw [hm]
    have hrlt : x.toNat % 256 < 256 := Nat.mod_lt _ (by decide)
    by_cases hz : x.toNat % 256 = 0
    · rw [hz]
      simp
    · have hpos : 1 ≤ x.toNat % 256 := by omega
      simp [hz, hpos]
  ·
    simp only [lane, BitVec.extractLsb'_toNat, Nat.shiftRight_eq_div_pow]
    change (fast x).getLsbD 15 = decide (x.toNat / 256 % 256 = 0)
    have ht := hhi
    simp [BitVec.getLsbD, Nat.testBit_eq_decide_div_mod_eq] at ht
    let b0 := x.toNat % 256
    let b1 := x.toNat / 256 % 256
    have hb0 : b0 < 256 := by dsimp [b0]; exact Nat.mod_lt _ (by decide)
    have hb1 : b1 < 256 := by dsimp [b1]; exact Nat.mod_lt _ (by decide)
    have hne_b : b1 ≠ 1 := by
      simpa [b1, lane, BitVec.extractLsb'_toNat, Nat.shiftRight_eq_div_pow] using hne
    have hlt : b1 < 128 := by dsimp [b1] at *; omega
    have hbmod : x.toNat / 256 % 128 = b1 := by
      dsimp [b1]
      rw [← Nat.mod_mod_of_dvd (x.toNat / 256) (show 128 ∣ 256 by decide)]
      exact Nat.mod_eq_of_lt (by simpa using hlt)
    have hdec : x.toNat % 32768 = b0 + b1 * 256 := by
      rw [show 32768 = 128 * 256 ^ 1 by native_decide]
      rw [mod_highByte]
      change lowBytes 1 x.toNat + 256 ^ 1 * (x.toNat / 256 ^ 1 % 128) = b0 + b1 * 256
      rw [hbmod]
      simp [lowBytes, b0, b1, Nat.mul_comm, Nat.mul_assoc]
    simp [fast, ones, high, ← BitVec.getLsbD_eq_getElem, BitVec.getLsbD_sub,
      BitVec.carry, hhi]
    change decide (257 ≤ x.toNat % 32768) = !decide (b1 = 0)
    rw [hdec]
    by_cases hz : b1 = 0
    · rw [hz]
      have hlow_lt : b0 < 256 := by omega
      have hP_lt_S : 256 < 257 := by omega
      have hnle : ¬ 257 ≤ b0 := Nat.not_le_of_gt (Nat.lt_trans hlow_lt hP_lt_S)
      simp [hnle]
    · have hpos : 2 ≤ b1 := by omega
      have hS_le : 257 ≤ 2 * 256 := by omega
      have hmul : 2 * 256 ≤ b1 * 256 := Nat.mul_le_mul_right 256 hpos
      have hle : 257 ≤ b0 + b1 * 256 := Nat.le_trans hS_le (Nat.le_trans hmul (Nat.le_add_left _ (b0)))
      simp [hz, hle]
  ·
    simp only [lane, BitVec.extractLsb'_toNat, Nat.shiftRight_eq_div_pow]
    change (fast x).getLsbD 23 = decide (x.toNat / 65536 % 256 = 0)
    have ht := hhi
    simp [BitVec.getLsbD, Nat.testBit_eq_decide_div_mod_eq] at ht
    let b0 := x.toNat % 256
    let b1 := x.toNat / 256 % 256
    let b2 := x.toNat / 65536 % 256
    have hb0 : b0 < 256 := by dsimp [b0]; exact Nat.mod_lt _ (by decide)
    have hb1 : b1 < 256 := by dsimp [b1]; exact Nat.mod_lt _ (by decide)
    have hb2 : b2 < 256 := by dsimp [b2]; exact Nat.mod_lt _ (by decide)
    have hne_b : b2 ≠ 1 := by
      simpa [b2, lane, BitVec.extractLsb'_toNat, Nat.shiftRight_eq_div_pow] using hne
    have hlt : b2 < 128 := by dsimp [b2] at *; omega
    have hbmod : x.toNat / 65536 % 128 = b2 := by
      dsimp [b2]
      rw [← Nat.mod_mod_of_dvd (x.toNat / 65536) (show 128 ∣ 256 by decide)]
      exact Nat.mod_eq_of_lt (by simpa using hlt)
    have hdec : x.toNat % 8388608 = b0 + b1 * 256 + b2 * 65536 := by
      rw [show 8388608 = 128 * 256 ^ 2 by native_decide]
      rw [mod_highByte]
      change lowBytes 2 x.toNat + 256 ^ 2 * (x.toNat / 256 ^ 2 % 128) = b0 + b1 * 256 + b2 * 65536
      rw [hbmod]
      simp [lowBytes, b0, b1, b2, Nat.mul_comm, Nat.mul_assoc]
    simp [fast, ones, high, ← BitVec.getLsbD_eq_getElem, BitVec.getLsbD_sub,
      BitVec.carry, hhi]
    change decide (65793 ≤ x.toNat % 8388608) = !decide (b2 = 0)
    rw [hdec]
    by_cases hz : b2 = 0
    · rw [hz]
      have hlow_lt : b0 + b1 * 256 < 65536 := by omega
      have hP_lt_S : 65536 < 65793 := by omega
      have hnle : ¬ 65793 ≤ b0 + b1 * 256 := Nat.not_le_of_gt (Nat.lt_trans hlow_lt hP_lt_S)
      simp [hnle]
    · have hpos : 2 ≤ b2 := by omega
      have hS_le : 65793 ≤ 2 * 65536 := by omega
      have hmul : 2 * 65536 ≤ b2 * 65536 := Nat.mul_le_mul_right 65536 hpos
      have hle : 65793 ≤ b0 + b1 * 256 + b2 * 65536 := Nat.le_trans hS_le (Nat.le_trans hmul (Nat.le_add_left _ (b0 + b1 * 256)))
      simp [hz, hle]
  ·
    simp only [lane, BitVec.extractLsb'_toNat, Nat.shiftRight_eq_div_pow]
    change (fast x).getLsbD 31 = decide (x.toNat / 16777216 % 256 = 0)
    have ht := hhi
    simp [BitVec.getLsbD, Nat.testBit_eq_decide_div_mod_eq] at ht
    let b0 := x.toNat % 256
    let b1 := x.toNat / 256 % 256
    let b2 := x.toNat / 65536 % 256
    let b3 := x.toNat / 16777216 % 256
    have hb0 : b0 < 256 := by dsimp [b0]; exact Nat.mod_lt _ (by decide)
    have hb1 : b1 < 256 := by dsimp [b1]; exact Nat.mod_lt _ (by decide)
    have hb2 : b2 < 256 := by dsimp [b2]; exact Nat.mod_lt _ (by decide)
    have hb3 : b3 < 256 := by dsimp [b3]; exact Nat.mod_lt _ (by decide)
    have hne_b : b3 ≠ 1 := by
      simpa [b3, lane, BitVec.extractLsb'_toNat, Nat.shiftRight_eq_div_pow] using hne
    have hlt : b3 < 128 := by dsimp [b3] at *; omega
    have hbmod : x.toNat / 16777216 % 128 = b3 := by
      dsimp [b3]
      rw [← Nat.mod_mod_of_dvd (x.toNat / 16777216) (show 128 ∣ 256 by decide)]
      exact Nat.mod_eq_of_lt (by simpa using hlt)
    have hdec : x.toNat % 2147483648 = b0 + b1 * 256 + b2 * 65536 + b3 * 16777216 := by
      rw [show 2147483648 = 128 * 256 ^ 3 by native_decide]
      rw [mod_highByte]
      change lowBytes 3 x.toNat + 256 ^ 3 * (x.toNat / 256 ^ 3 % 128) = b0 + b1 * 256 + b2 * 65536 + b3 * 16777216
      rw [hbmod]
      simp [lowBytes, b0, b1, b2, b3, Nat.mul_comm, Nat.mul_assoc]
    simp [fast, ones, high, ← BitVec.getLsbD_eq_getElem, BitVec.getLsbD_sub,
      BitVec.carry, hhi]
    change decide (16843009 ≤ x.toNat % 2147483648) = !decide (b3 = 0)
    rw [hdec]
    by_cases hz : b3 = 0
    · rw [hz]
      have hlow_lt : b0 + b1 * 256 + b2 * 65536 < 16777216 := by omega
      have hP_lt_S : 16777216 < 16843009 := by omega
      have hnle : ¬ 16843009 ≤ b0 + b1 * 256 + b2 * 65536 := Nat.not_le_of_gt (Nat.lt_trans hlow_lt hP_lt_S)
      simp [hnle]
    · have hpos : 2 ≤ b3 := by omega
      have hS_le : 16843009 ≤ 2 * 16777216 := by omega
      have hmul : 2 * 16777216 ≤ b3 * 16777216 := Nat.mul_le_mul_right 16777216 hpos
      have hle : 16843009 ≤ b0 + b1 * 256 + b2 * 65536 + b3 * 16777216 := Nat.le_trans hS_le (Nat.le_trans hmul (Nat.le_add_left _ (b0 + b1 * 256 + b2 * 65536)))
      simp [hz, hle]
  ·
    simp only [lane, BitVec.extractLsb'_toNat, Nat.shiftRight_eq_div_pow]
    change (fast x).getLsbD 39 = decide (x.toNat / 4294967296 % 256 = 0)
    have ht := hhi
    simp [BitVec.getLsbD, Nat.testBit_eq_decide_div_mod_eq] at ht
    let b0 := x.toNat % 256
    let b1 := x.toNat / 256 % 256
    let b2 := x.toNat / 65536 % 256
    let b3 := x.toNat / 16777216 % 256
    let b4 := x.toNat / 4294967296 % 256
    have hb0 : b0 < 256 := by dsimp [b0]; exact Nat.mod_lt _ (by decide)
    have hb1 : b1 < 256 := by dsimp [b1]; exact Nat.mod_lt _ (by decide)
    have hb2 : b2 < 256 := by dsimp [b2]; exact Nat.mod_lt _ (by decide)
    have hb3 : b3 < 256 := by dsimp [b3]; exact Nat.mod_lt _ (by decide)
    have hb4 : b4 < 256 := by dsimp [b4]; exact Nat.mod_lt _ (by decide)
    have hne_b : b4 ≠ 1 := by
      simpa [b4, lane, BitVec.extractLsb'_toNat, Nat.shiftRight_eq_div_pow] using hne
    have hlt : b4 < 128 := by dsimp [b4] at *; omega
    have hbmod : x.toNat / 4294967296 % 128 = b4 := by
      dsimp [b4]
      rw [← Nat.mod_mod_of_dvd (x.toNat / 4294967296) (show 128 ∣ 256 by decide)]
      exact Nat.mod_eq_of_lt (by simpa using hlt)
    have hdec : x.toNat % 549755813888 = b0 + b1 * 256 + b2 * 65536 + b3 * 16777216 + b4 * 4294967296 := by
      rw [show 549755813888 = 128 * 256 ^ 4 by native_decide]
      rw [mod_highByte]
      change lowBytes 4 x.toNat + 256 ^ 4 * (x.toNat / 256 ^ 4 % 128) = b0 + b1 * 256 + b2 * 65536 + b3 * 16777216 + b4 * 4294967296
      rw [hbmod]
      simp [lowBytes, b0, b1, b2, b3, b4, Nat.mul_comm, Nat.mul_assoc]
    have hhighi : high.getLsbD 39 = true := by native_decide
    have hybit : (~~~ones + (1#64)).getLsbD 39 = true := by native_decide
    unfold fast
    simp only [BitVec.getLsbD_and, BitVec.getLsbD_not, hhi, hhighi, Bool.false_and, Bool.and_true]
    rw [BitVec.getLsbD_sub (i_lt := by decide)]
    simp [← BitVec.getLsbD_eq_getElem, hhi]
    rw [hybit]
    simp
    unfold BitVec.carry
    have hy : ((~~~ones + (1#64)).toNat % 2 ^ 39) = 2 ^ 39 - 4311810305 := by native_decide
    rw [hy]
    have hpow : 2 ^ 39 = 549755813888 := by native_decide
    rw [hpow]
    rw [show false.toNat = 0 by rfl]
    simp only [Nat.add_zero]
    have hxlt : x.toNat % 549755813888 < 549755813888 := Nat.mod_lt _ (by decide)
    have hSlt : 4311810305 < 549755813888 := by native_decide
    have hconv : (x.toNat % 549755813888 + (549755813888 - 4311810305) ≥ 549755813888) ↔ (4311810305 ≤ x.toNat % 549755813888) := by
      constructor <;> intro h <;> omega
    simp [hconv]
    change decide (4311810305 ≤ x.toNat % 549755813888) = !decide (b4 = 0)
    rw [hdec]
    by_cases hz : b4 = 0
    · have hlow_lt : b0 + b1 * 256 + b2 * 65536 + b3 * 16777216 < 4294967296 := by omega
      have hP_lt_S : 4294967296 < 4311810305 := by omega
      have hnle : ¬ 4311810305 ≤ b0 + b1 * 256 + b2 * 65536 + b3 * 16777216 + b4 * 4294967296 := by omega
      apply Bool.eq_iff_iff.mpr
      rw [decide_eq_true_eq, not_decide_eq_true]
      constructor
      · intro hp
        exact False.elim (hnle hp)
      · intro hnq
        exact False.elim (hnq hz)
    · have hpos : 2 ≤ b4 := two_le_of_ne_zero_one b4 hz hne_b
      have hS_le : 4311810305 ≤ 2 * 4294967296 := by omega
      have hmul : 2 * 4294967296 ≤ b4 * 4294967296 := Nat.mul_le_mul_right 4294967296 hpos
      have hle : 4311810305 ≤ b0 + b1 * 256 + b2 * 65536 + b3 * 16777216 + b4 * 4294967296 := Nat.le_trans hS_le (Nat.le_trans hmul (Nat.le_add_left _ (b0 + b1 * 256 + b2 * 65536 + b3 * 16777216)))
      apply Bool.eq_iff_iff.mpr
      rw [decide_eq_true_eq, not_decide_eq_true]
      constructor
      · intro hp
        exact hz
      · intro hnq
        exact hle
  ·
    simp only [lane, BitVec.extractLsb'_toNat, Nat.shiftRight_eq_div_pow]
    change (fast x).getLsbD 47 = decide (x.toNat / 1099511627776 % 256 = 0)
    have ht := hhi
    simp [BitVec.getLsbD, Nat.testBit_eq_decide_div_mod_eq] at ht
    let b0 := x.toNat % 256
    let b1 := x.toNat / 256 % 256
    let b2 := x.toNat / 65536 % 256
    let b3 := x.toNat / 16777216 % 256
    let b4 := x.toNat / 4294967296 % 256
    let b5 := x.toNat / 1099511627776 % 256
    have hb0 : b0 < 256 := by dsimp [b0]; exact Nat.mod_lt _ (by decide)
    have hb1 : b1 < 256 := by dsimp [b1]; exact Nat.mod_lt _ (by decide)
    have hb2 : b2 < 256 := by dsimp [b2]; exact Nat.mod_lt _ (by decide)
    have hb3 : b3 < 256 := by dsimp [b3]; exact Nat.mod_lt _ (by decide)
    have hb4 : b4 < 256 := by dsimp [b4]; exact Nat.mod_lt _ (by decide)
    have hb5 : b5 < 256 := by dsimp [b5]; exact Nat.mod_lt _ (by decide)
    have hne_b : b5 ≠ 1 := by
      simpa [b5, lane, BitVec.extractLsb'_toNat, Nat.shiftRight_eq_div_pow] using hne
    have hlt : b5 < 128 := by dsimp [b5] at *; omega
    have hbmod : x.toNat / 1099511627776 % 128 = b5 := by
      dsimp [b5]
      rw [← Nat.mod_mod_of_dvd (x.toNat / 1099511627776) (show 128 ∣ 256 by decide)]
      exact Nat.mod_eq_of_lt (by simpa using hlt)
    have hdec : x.toNat % 140737488355328 = b0 + b1 * 256 + b2 * 65536 + b3 * 16777216 + b4 * 4294967296 + b5 * 1099511627776 := by
      rw [show 140737488355328 = 128 * 256 ^ 5 by native_decide]
      rw [mod_highByte]
      change lowBytes 5 x.toNat + 256 ^ 5 * (x.toNat / 256 ^ 5 % 128) = b0 + b1 * 256 + b2 * 65536 + b3 * 16777216 + b4 * 4294967296 + b5 * 1099511627776
      rw [hbmod]
      simp [lowBytes, b0, b1, b2, b3, b4, b5, Nat.mul_comm, Nat.mul_assoc]
    simp [fast, ones, high, ← BitVec.getLsbD_eq_getElem, BitVec.getLsbD_sub,
      BitVec.carry, hhi]
    change decide (1103823438081 ≤ x.toNat % 140737488355328) = !decide (b5 = 0)
    rw [hdec]
    by_cases hz : b5 = 0
    · rw [hz]
      have hlow_lt : b0 + b1 * 256 + b2 * 65536 + b3 * 16777216 + b4 * 4294967296 < 1099511627776 := by omega
      have hP_lt_S : 1099511627776 < 1103823438081 := by omega
      have hnle : ¬ 1103823438081 ≤ b0 + b1 * 256 + b2 * 65536 + b3 * 16777216 + b4 * 4294967296 := Nat.not_le_of_gt (Nat.lt_trans hlow_lt hP_lt_S)
      simp [hnle]
    · have hpos : 2 ≤ b5 := by omega
      have hS_le : 1103823438081 ≤ 2 * 1099511627776 := by omega
      have hmul : 2 * 1099511627776 ≤ b5 * 1099511627776 := Nat.mul_le_mul_right 1099511627776 hpos
      have hle : 1103823438081 ≤ b0 + b1 * 256 + b2 * 65536 + b3 * 16777216 + b4 * 4294967296 + b5 * 1099511627776 := Nat.le_trans hS_le (Nat.le_trans hmul (Nat.le_add_left _ (b0 + b1 * 256 + b2 * 65536 + b3 * 16777216 + b4 * 4294967296)))
      simp [hz, hle]
  ·
    simp only [lane, BitVec.extractLsb'_toNat, Nat.shiftRight_eq_div_pow]
    change (fast x).getLsbD 55 = decide (x.toNat / 281474976710656 % 256 = 0)
    have ht := hhi
    simp [BitVec.getLsbD, Nat.testBit_eq_decide_div_mod_eq] at ht
    let b0 := x.toNat % 256
    let b1 := x.toNat / 256 % 256
    let b2 := x.toNat / 65536 % 256
    let b3 := x.toNat / 16777216 % 256
    let b4 := x.toNat / 4294967296 % 256
    let b5 := x.toNat / 1099511627776 % 256
    let b6 := x.toNat / 281474976710656 % 256
    have hb0 : b0 < 256 := by dsimp [b0]; exact Nat.mod_lt _ (by decide)
    have hb1 : b1 < 256 := by dsimp [b1]; exact Nat.mod_lt _ (by decide)
    have hb2 : b2 < 256 := by dsimp [b2]; exact Nat.mod_lt _ (by decide)
    have hb3 : b3 < 256 := by dsimp [b3]; exact Nat.mod_lt _ (by decide)
    have hb4 : b4 < 256 := by dsimp [b4]; exact Nat.mod_lt _ (by decide)
    have hb5 : b5 < 256 := by dsimp [b5]; exact Nat.mod_lt _ (by decide)
    have hb6 : b6 < 256 := by dsimp [b6]; exact Nat.mod_lt _ (by decide)
    have hne_b : b6 ≠ 1 := by
      simpa [b6, lane, BitVec.extractLsb'_toNat, Nat.shiftRight_eq_div_pow] using hne
    have hlt : b6 < 128 := by dsimp [b6] at *; omega
    have hbmod : x.toNat / 281474976710656 % 128 = b6 := by
      dsimp [b6]
      rw [← Nat.mod_mod_of_dvd (x.toNat / 281474976710656) (show 128 ∣ 256 by decide)]
      exact Nat.mod_eq_of_lt (by simpa using hlt)
    have hdec : x.toNat % 36028797018963968 = b0 + b1 * 256 + b2 * 65536 + b3 * 16777216 + b4 * 4294967296 + b5 * 1099511627776 + b6 * 281474976710656 := by
      rw [show 36028797018963968 = 128 * 256 ^ 6 by native_decide]
      rw [mod_highByte]
      change lowBytes 6 x.toNat + 256 ^ 6 * (x.toNat / 256 ^ 6 % 128) = b0 + b1 * 256 + b2 * 65536 + b3 * 16777216 + b4 * 4294967296 + b5 * 1099511627776 + b6 * 281474976710656
      rw [hbmod]
      simp [lowBytes, b0, b1, b2, b3, b4, b5, b6, Nat.mul_comm, Nat.mul_assoc]
    simp [fast, ones, high, ← BitVec.getLsbD_eq_getElem, BitVec.getLsbD_sub,
      BitVec.carry, hhi]
    change decide (282578800148737 ≤ x.toNat % 36028797018963968) = !decide (b6 = 0)
    rw [hdec]
    by_cases hz : b6 = 0
    · rw [hz]
      have hlow_lt : b0 + b1 * 256 + b2 * 65536 + b3 * 16777216 + b4 * 4294967296 + b5 * 1099511627776 < 281474976710656 := by omega
      have hP_lt_S : 281474976710656 < 282578800148737 := by omega
      have hnle : ¬ 282578800148737 ≤ b0 + b1 * 256 + b2 * 65536 + b3 * 16777216 + b4 * 4294967296 + b5 * 1099511627776 := Nat.not_le_of_gt (Nat.lt_trans hlow_lt hP_lt_S)
      simp [hnle]
    · have hpos : 2 ≤ b6 := by omega
      have hS_le : 282578800148737 ≤ 2 * 281474976710656 := by omega
      have hmul : 2 * 281474976710656 ≤ b6 * 281474976710656 := Nat.mul_le_mul_right 281474976710656 hpos
      have hle : 282578800148737 ≤ b0 + b1 * 256 + b2 * 65536 + b3 * 16777216 + b4 * 4294967296 + b5 * 1099511627776 + b6 * 281474976710656 := Nat.le_trans hS_le (Nat.le_trans hmul (Nat.le_add_left _ (b0 + b1 * 256 + b2 * 65536 + b3 * 16777216 + b4 * 4294967296 + b5 * 1099511627776)))
      simp [hz, hle]
  ·
    simp only [lane, BitVec.extractLsb'_toNat, Nat.shiftRight_eq_div_pow]
    change (fast x).getLsbD 63 = decide (x.toNat / 72057594037927936 % 256 = 0)
    have ht := hhi
    simp [BitVec.getLsbD, Nat.testBit_eq_decide_div_mod_eq] at ht
    let b0 := x.toNat % 256
    let b1 := x.toNat / 256 % 256
    let b2 := x.toNat / 65536 % 256
    let b3 := x.toNat / 16777216 % 256
    let b4 := x.toNat / 4294967296 % 256
    let b5 := x.toNat / 1099511627776 % 256
    let b6 := x.toNat / 281474976710656 % 256
    let b7 := x.toNat / 72057594037927936 % 256
    have hb0 : b0 < 256 := by dsimp [b0]; exact Nat.mod_lt _ (by decide)
    have hb1 : b1 < 256 := by dsimp [b1]; exact Nat.mod_lt _ (by decide)
    have hb2 : b2 < 256 := by dsimp [b2]; exact Nat.mod_lt _ (by decide)
    have hb3 : b3 < 256 := by dsimp [b3]; exact Nat.mod_lt _ (by decide)
    have hb4 : b4 < 256 := by dsimp [b4]; exact Nat.mod_lt _ (by decide)
    have hb5 : b5 < 256 := by dsimp [b5]; exact Nat.mod_lt _ (by decide)
    have hb6 : b6 < 256 := by dsimp [b6]; exact Nat.mod_lt _ (by decide)
    have hb7 : b7 < 256 := by dsimp [b7]; exact Nat.mod_lt _ (by decide)
    have hne_b : b7 ≠ 1 := by
      simpa [b7, lane, BitVec.extractLsb'_toNat, Nat.shiftRight_eq_div_pow] using hne
    have hlt : b7 < 128 := by dsimp [b7] at *; omega
    have hbmod : x.toNat / 72057594037927936 % 128 = b7 := by
      dsimp [b7]
      rw [← Nat.mod_mod_of_dvd (x.toNat / 72057594037927936) (show 128 ∣ 256 by decide)]
      exact Nat.mod_eq_of_lt (by simpa using hlt)
    have hdec : x.toNat % 9223372036854775808 = b0 + b1 * 256 + b2 * 65536 + b3 * 16777216 + b4 * 4294967296 + b5 * 1099511627776 + b6 * 281474976710656 + b7 * 72057594037927936 := by
      rw [show 9223372036854775808 = 128 * 256 ^ 7 by native_decide]
      rw [mod_highByte]
      change lowBytes 7 x.toNat + 256 ^ 7 * (x.toNat / 256 ^ 7 % 128) = b0 + b1 * 256 + b2 * 65536 + b3 * 16777216 + b4 * 4294967296 + b5 * 1099511627776 + b6 * 281474976710656 + b7 * 72057594037927936
      rw [hbmod]
      simp [lowBytes, b0, b1, b2, b3, b4, b5, b6, b7, Nat.mul_comm, Nat.mul_assoc]
    simp [fast, ones, high, ← BitVec.getLsbD_eq_getElem, BitVec.getLsbD_sub,
      BitVec.carry, hhi]
    change decide (72340172838076673 ≤ x.toNat % 9223372036854775808) = !decide (b7 = 0)
    rw [hdec]
    by_cases hz : b7 = 0
    · rw [hz]
      have hlow_lt : b0 + b1 * 256 + b2 * 65536 + b3 * 16777216 + b4 * 4294967296 + b5 * 1099511627776 + b6 * 281474976710656 < 72057594037927936 := by omega
      have hP_lt_S : 72057594037927936 < 72340172838076673 := by omega
      have hnle : ¬ 72340172838076673 ≤ b0 + b1 * 256 + b2 * 65536 + b3 * 16777216 + b4 * 4294967296 + b5 * 1099511627776 + b6 * 281474976710656 := Nat.not_le_of_gt (Nat.lt_trans hlow_lt hP_lt_S)
      simp [hnle]
    · have hpos : 2 ≤ b7 := by omega
      have hS_le : 72340172838076673 ≤ 2 * 72057594037927936 := by omega
      have hmul : 2 * 72057594037927936 ≤ b7 * 72057594037927936 := Nat.mul_le_mul_right 72057594037927936 hpos
      have hle : 72340172838076673 ≤ b0 + b1 * 256 + b2 * 65536 + b3 * 16777216 + b4 * 4294967296 + b5 * 1099511627776 + b6 * 281474976710656 + b7 * 72057594037927936 := Nat.le_trans hS_le (Nat.le_trans hmul (Nat.le_add_left _ (b0 + b1 * 256 + b2 * 65536 + b3 * 16777216 + b4 * 4294967296 + b5 * 1099511627776 + b6 * 281474976710656)))
      simp [hz, hle]

theorem sound : TinylambOpt.ResearchGoals.JitKernels.BorrowMasks.BorrowImpossibleNeighbor.Goal := by
  intro x hx
  ext j
  have hj : j < 64 := by omega
  simp only [← BitVec.getLsbD_eq_getElem]
  by_cases hmod : j % 8 = 7
  · let k := j / 8
    have hk : k < 8 := by omega
    have hjeq : 8 * k + 7 = j := by omega
    rw [← hjeq]
    by_cases hhi : x.getLsbD (8 * k + 7) = true
    · have hlane : lane x k ≠ 0 := lane_ne_zero_of_high x k hk hhi
      rw [zeroBytes_lsb_lane x k hk]
      simp [hlane, fast, ← BitVec.getLsbD_eq_getElem, hhi]
    · have hfalse : x.getLsbD (8 * k + 7) = false := by
        cases hbit : x.getLsbD (8 * k + 7)
        · rfl
        · exact False.elim (hhi hbit)
      rw [zeroBytes_lsb_lane x k hk, fast_high_lsb x k hk (hx k hk) hfalse]
  · rw [high_only_false (fast x) (fast_high_only x) j hj hmod,
        high_only_false (zeroBytes x) (zeroBytes_high_only x) j hj hmod]

end TinylambOpt.ResearchProofs.JitKernels.Goal_BorrowMasks_BorrowImpossibleNeighbor
