import TinylambOpt.ResearchGoals.JitKernels.NullWordFusion

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_NullWordFusion_PaddingBitComplementCount

open TinylambOpt.ResearchGoals.JitKernels

theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.NullWordFusion.PaddingBitComplementCount.Goal := by
  intro h
  have c := h 0 0 (by decide) (by decide)
  exact (by native_decide : ¬ (
    (NullWordFusion.ords (NullWordFusion.valid 0 0)).length = 64 - (NullWordFusion.ords 0).length)) c

end TinylambOpt.ResearchProofs.JitKernels.Goal_NullWordFusion_PaddingBitComplementCount
