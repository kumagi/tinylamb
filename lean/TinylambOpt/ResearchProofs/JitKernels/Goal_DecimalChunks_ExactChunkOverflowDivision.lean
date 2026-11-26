import TinylambOpt.ResearchGoals.JitKernels.DecimalChunks

/-! A nonnegative decimal append can be checked against a bound by comparing
the low chunk with the capacity and then the high chunk with the remaining
quotient. This is exactly Nat division by a positive power of ten, so SQL can
perform the guard before the multiply. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.DecimalChunks.Goal_ExactChunkOverflowDivision

open TinylambOpt.ResearchGoals.JitKernels.DecimalChunks

theorem sound : ExactChunkOverflowDivision.Goal := by
  intro a b cap k hb
  have hp : 0 < 10 ^ k := Nat.pow_pos (a := 10) (n := k) (by decide)
  constructor
  · intro hle
    have hbcap : b ≤ cap := by omega
    have hmul : a * 10 ^ k ≤ cap - b := by omega
    exact ⟨hbcap, (Nat.le_div_iff_mul_le hp).2 hmul⟩
  · rintro ⟨hbcap, hadiv⟩
    have hmul : a * 10 ^ k ≤ cap - b := (Nat.le_div_iff_mul_le hp).1 hadiv
    omega

end TinylambOpt.ResearchProofs.JitKernels.DecimalChunks.Goal_ExactChunkOverflowDivision
