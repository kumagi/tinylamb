import TinylambOpt.ResearchGoals.JitKernels.NullWordFusion

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_NullWordFusion_BranchNullMaskMux

theorem sound : TinylambOpt.ResearchGoals.JitKernels.NullWordFusion.BranchNullMaskMux.Goal := by
  intro truth predNull thenNull elseNull i hi
  dsimp
  simp only [BitVec.getLsbD_eq_getElem hi]
  by_cases ht : truth[i] <;> by_cases hp : predNull[i] <;> simp [ht, hp]

end TinylambOpt.ResearchProofs.JitKernels.Goal_NullWordFusion_BranchNullMaskMux
