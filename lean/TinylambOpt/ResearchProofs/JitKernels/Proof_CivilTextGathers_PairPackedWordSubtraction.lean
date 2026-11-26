import TinylambOpt.ResearchGoals.JitKernels.CivilTextGathers

/-! Packed ASCII subtraction reproduces the two decimal lanes, allowing one word operation instead of two scalar reads. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace Nov26.Proof_CivilTextGathers_PairPackedWordSubtraction

open TinylambOpt.ResearchGoals.JitKernels.CivilTextGathers
open TinylambOpt.ResearchGoals.JitKernels.CivilTextGathers.PairPackedWordSubtraction

theorem sound : Goal := by
  intro a b ha ha' hb hb'
  dsimp
  have hword : a + 256 * b - 0x3030 = (a - 48) + 256 * (b - 48) := by omega
  rw [hword]
  simp [pair, Nat.add_mul_div_right, Nat.add_mul_mod_self_left]
  omega

end Nov26.Proof_CivilTextGathers_PairPackedWordSubtraction
