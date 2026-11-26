import TinylambOpt.ResearchGoals.JitKernels.NullWordFusion

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_NullWordFusion_BranchNullMaskMux

open TinylambOpt.ResearchGoals.JitKernels.NullWordFusion

theorem sound : BranchNullMaskMux.Goal := by
  intro truth predNull thenNull elseNull i hi
  simp only [BitVec.getLsbD_and, BitVec.getLsbD_or, BitVec.getLsbD_not]
  rw [show decide (i < 64) = true from decide_eq_true hi]
  cases h : truth.getLsbD i <;> cases hp : predNull.getLsbD i <;>
    cases ht : thenNull.getLsbD i <;> cases he : elseNull.getLsbD i <;>
    simp [h, hp, ht, he]

end TinylambOpt.ResearchProofs.JitKernels.Goal_NullWordFusion_BranchNullMaskMux
