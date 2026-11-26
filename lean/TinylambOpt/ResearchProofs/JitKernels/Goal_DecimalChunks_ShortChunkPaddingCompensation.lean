import TinylambOpt.ResearchGoals.JitKernels.DecimalChunks

/-! Right-zero padding appends ten-digit steps whose digit contribution is zero.
The resulting magnitude is the original value times a power of ten, so
rescaling by integer division recovers it; SQL may decode padded chunks with a
constant compensation. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_DecimalChunks_ShortChunkPaddingCompensation

open TinylambOpt.ResearchGoals.JitKernels.DecimalChunks

theorem foldlDigitAccumulator (n : Nat) (b : List Nat) :
    List.foldl (fun n c => 10 * n + (c - 48)) n b = n * 10 ^ b.length + magnitude b := by
  induction b generalizing n with
  | nil => simp [magnitude]
  | cons c b ih =>
      dsimp [magnitude]
      simp only [Nat.zero_add]
      rw [ih (n := 10 * n + (c - 48))]
      rw [ih (n := c - 48)]
      rw [Nat.pow_succ]
      simp only [Nat.mul_add, Nat.mul_assoc, Nat.mul_comm, Nat.add_assoc]

theorem magnitudeAppend (a b : List Nat) :
    magnitude (a ++ b) = magnitude a * 10 ^ b.length + magnitude b := by
  unfold magnitude
  rw [List.foldl_append]
  exact foldlDigitAccumulator (magnitude a) b

theorem magnitudeReplicateZero : ∀ n, magnitude (List.replicate n 48) = 0 := by
  intro n
  induction n with
  | zero => simp [magnitude]
  | succ n ih => simpa [magnitude, List.replicate_succ] using ih

theorem sound : TinylambOpt.ResearchGoals.JitKernels.DecimalChunks.ShortChunkPaddingCompensation.Goal := by
  intro s n
  rw [magnitudeAppend, magnitudeReplicateZero, List.length_replicate, Nat.add_zero]
  exact Nat.mul_div_cancel (magnitude s) (Nat.pow_pos (a := 10) (n := n) (by decide))

end TinylambOpt.ResearchProofs.JitKernels.Goal_DecimalChunks_ShortChunkPaddingCompensation