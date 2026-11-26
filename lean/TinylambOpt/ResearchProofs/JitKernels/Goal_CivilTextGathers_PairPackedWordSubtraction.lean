import TinylambOpt.ResearchGoals.JitKernels.CivilTextGathers

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_CivilTextGathers_PairPackedWordSubtraction

/-- Subtracting the ASCII-zero word separates the two decimal nibbles without
any carry across lanes, so the packed word decodes to the same two digits. -/
theorem sound : TinylambOpt.ResearchGoals.JitKernels.CivilTextGathers.PairPackedWordSubtraction.Goal := by
  intro a b ha ha' hb hb'
  dsimp
  have hword : a + 256 * b - 0x3030 = (a - 48) + 256 * (b - 48) := by omega
  rw [hword]
  simp [TinylambOpt.ResearchGoals.JitKernels.CivilTextGathers.pair]
  omega

end TinylambOpt.ResearchProofs.JitKernels.Goal_CivilTextGathers_PairPackedWordSubtraction
