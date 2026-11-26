import TinylambOpt.ResearchGoals.JitKernels.CivilTextGathers

/-! Subtracting the packed ASCII-zero constant from a two-digit word yields one
byte per decimal digit, so the two lane extractions recover the same component
value. The proof keeps the modeled `pair` result unchanged. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_CivilTextGathers_PairPackedWordSubtraction

open TinylambOpt.ResearchGoals.JitKernels.CivilTextGathers

theorem sound : PairPackedWordSubtraction.Goal := by
  intro a b ha ha' hb hb'
  dsimp
  have hword : a + 256 * b - 0x3030 = (a - 48) + 256 * (b - 48) := by omega
  rw [hword]
  simp [pair, Nat.add_mul_mod_self_left]
  omega

end TinylambOpt.ResearchProofs.JitKernels.Goal_CivilTextGathers_PairPackedWordSubtraction
