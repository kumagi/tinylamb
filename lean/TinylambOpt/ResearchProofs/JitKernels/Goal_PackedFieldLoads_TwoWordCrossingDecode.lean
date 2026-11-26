import TinylambOpt.ResearchGoals.JitKernels.PackedFieldLoads

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_PackedFieldLoads_TwoWordCrossingDecode

open TinylambOpt.ResearchGoals.JitKernels.PackedFieldLoads
open TinylambOpt.ResearchGoals.Compression.Wire


theorem fromLittle_take_add (xs : List Nat) (n k : Nat) :
    fromLittle (xs.take (n+k)) = fromLittle (xs.take n) + 256^n * fromLittle ((xs.drop n).take k) := by
  induction n generalizing xs with
  | zero => simp [fromLittle]
  | succ n ih =>
      cases xs with
      | nil => simp [fromLittle]
      | cons b rest =>
          rw [show n + 1 + k = (n + k) + 1 by omega]
          simp [fromLittle, ih, Nat.pow_succ, Nat.mul_add, Nat.mul_assoc, Nat.mul_comm, Nat.mul_left_comm]
          omega

theorem mod_pow_cancel (A B : Nat) (shift w k : Nat) (hk : shift + w ≤ 8*k) :
    (A + 256^k * B) / 2^shift % 2^w = A / 2^shift % 2^w := by
  have h256 : (256:Nat) = 2^8 := by decide
  have hpow : 256^k = 2^shift * (2^w * 2^(8*k - shift - w)) := by
    rw [h256]
    rw [← Nat.pow_mul]
    have h8 : 8*k = shift + w + (8*k - shift - w) := by omega
    rw [h8, Nat.pow_add, Nat.pow_add]
    rw [show shift + w + (8*k - shift - w) - shift - w = 8*k - shift - w by omega]
    simp [Nat.mul_assoc]
  rw [hpow]
  rw [show 2^shift * (2^w * 2^(8*k - shift - w)) * B = 2^shift * ((2^w * 2^(8*k - shift - w)) * B) by simp [Nat.mul_assoc]]
  rw [Nat.add_mul_div_left A ((2^w * 2^(8*k - shift - w)) * B) (y := 2^shift) (Nat.two_pow_pos shift)]
  rw [show 2^w * 2^(8*k - shift - w) * B = 2^w * (2^(8*k-shift-w)*B) by simp [Nat.mul_assoc]]
  rw [Nat.add_mul_mod_self_left]

theorem sound : TinylambOpt.ResearchGoals.JitKernels.PackedFieldLoads.TwoWordCrossingDecode.Goal := by
  intro bs w i hw hcanon
  unfold load takeAt
  let s := i*w/8
  let shift := i*w%8
  let k := byteCount (shift+w)
  have hshift : shift < 8 := by
    dsimp [shift]
    exact Nat.mod_lt _ (by omega : 0 < 8)
  have hk8 : shift + w ≤ 8*k := by
    dsimp [k, byteCount]
    omega
  have hk16 : k ≤ 16 := by
    dsimp [k, byteCount]
    omega
  have hsplit : fromLittle ((bs.drop s).take 16) =
      fromLittle ((bs.drop s).take k) + 256^k * fromLittle (((bs.drop s).drop k).take (16-k)) := by
    have h := fromLittle_take_add (bs.drop s) k (16-k)
    have hkn : k + (16-k) = 16 := by omega
    rw [hkn] at h
    exact h
  rw [hsplit]
  exact mod_pow_cancel _ _ shift w k hk8

end TinylambOpt.ResearchProofs.JitKernels.Goal_PackedFieldLoads_TwoWordCrossingDecode
