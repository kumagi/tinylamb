import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_BitwiseXor_AlignedSiblings

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.BitwiseXor

private theorem xor_two_pow (k a b : Nat) : a * 2 ^ k ^^^ b * 2 ^ k = (a ^^^ b) * 2 ^ k := by
  refine Nat.eq_of_testBit_eq fun i => ?_
  simp only [Nat.testBit_xor, Nat.testBit_mul_two_pow]
  by_cases hd : k ≤ i
  · simp [hd]
  · simp [hd]

private theorem width_two_pow_sub_one (k : Nat) : width (2 ^ k - 1) = k := by
  cases k with
  | zero => rfl
  | succ j =>
    have hP : 2 ^ (j + 1) = 2 ^ j * 2 := Nat.pow_succ 2 j
    have hq : 1 ≤ 2 ^ j := Nat.one_le_two_pow
    have hne : 2 ^ (j + 1) - 1 ≠ 0 := by
      rw [hP]
      omega
    have hlog : (2 ^ (j + 1) - 1).log2 = j := by
      refine (Nat.log2_eq_iff hne).mpr ⟨?_, ?_⟩ <;> rw [hP] <;> omega
    simp [width, hne, hlog]

private theorem xor_corner (k a : Nat) :
    a * 2 ^ k ^^^ (a * 2 ^ k + (2 ^ k - 1)) = 2 ^ k - 1 := by
  have hl : 2 ^ k - 1 < 2 ^ k :=
    Nat.sub_lt_of_pos_le (by decide) Nat.one_le_two_pow
  have ho : 2 ^ k * a + (2 ^ k - 1) = 2 ^ k * a ||| (2 ^ k - 1) :=
    Nat.two_pow_add_eq_or_of_lt hl a
  rw [show a * 2 ^ k = 2 ^ k * a from Nat.mul_comm _ _, ho]
  refine Nat.eq_of_testBit_eq fun i => ?_
  rw [Nat.testBit_xor, Nat.testBit_or, Nat.testBit_two_pow_sub_one, Nat.testBit_two_pow_mul]
  by_cases hd : i < k
  · have hn : ¬ k ≤ i := Nat.not_le_of_lt hd
    simp [hd, hn]
  · have hk : k ≤ i := Nat.le_of_not_lt hd
    simp [hd, hk]

theorem sound_AlignedSiblings : BitwiseXor.AlignedSiblings.Goal := by
  intro k a b
  let A : Bounds := ⟨a * 2 ^ k, (a + 1) * 2 ^ k - 1⟩
  let B : Bounds := ⟨b * 2 ^ k, (b + 1) * 2 ^ k - 1⟩
  have corner (c : Nat) : c * 2 ^ k ^^^ ((c + 1) * 2 ^ k - 1) = 2 ^ k - 1 := by
    have hsucc : (c + 1) * 2 ^ k = c * 2 ^ k + 2 ^ k := by
      rw [Nat.add_mul, Nat.one_mul]
    rw [hsucc]
    have hsub : c * 2 ^ k + 2 ^ k - 1 = c * 2 ^ k + (2 ^ k - 1) := by
      have hp : 0 < 2 ^ k := Nat.two_pow_pos k
      omega
    rw [hsub]
    exact xor_corner k c
  have hA : A.lo ^^^ A.hi = 2 ^ k - 1 := by
    dsimp [A]
    exact corner a
  have hB : B.lo ^^^ B.hi = 2 ^ k - 1 := by
    dsimp [B]
    exact corner b
  have hw : max (width (A.lo ^^^ A.hi)) (width (B.lo ^^^ B.hi)) = k := by
    simp [hA, hB, width_two_pow_sub_one]
  have hxor : A.lo ^^^ B.lo = (a ^^^ b) * 2 ^ k := by
    dsimp [A, B]
    exact xor_two_pow k a b
  change ((A.lo ^^^ B.lo) / 2 ^ max (width (A.lo ^^^ A.hi)) (width (B.lo ^^^ B.hi))) *
      2 ^ max (width (A.lo ^^^ A.hi)) (width (B.lo ^^^ B.hi)) = (a ^^^ b) * 2 ^ k
  rw [hw, hxor, Nat.mul_div_left _ (Nat.two_pow_pos k)]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_BitwiseXor_AlignedSiblings
