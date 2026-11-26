import TinylambOpt.ResearchGoals.JitKernels.DecimalChunks

/-! A decimal chunk append is a left fold of `10*n + digit`. Splitting the fold
at the chunk boundary yields one multiply-add by a power of ten: SQL may parse
digit groups independently and concatenate them without changing the value. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.DecimalChunks.Goal_DecimalBlockMultiplyAccumulate

open TinylambOpt.ResearchGoals.JitKernels.DecimalChunks

/-- The accumulator after a fold over `b` started at `n`. -/
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

theorem sound : DecimalBlockMultiplyAccumulate.Goal := by
  intro a b
  unfold magnitude
  rw [List.foldl_append]
  exact foldlDigitAccumulator (magnitude a) b

end TinylambOpt.ResearchProofs.JitKernels.DecimalChunks.Goal_DecimalBlockMultiplyAccumulate
