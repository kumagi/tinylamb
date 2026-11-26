import TinylambOpt.ResearchGoals.JitKernels.NullWordFusion

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_NullWordFusion_PaddingBitComplementCount

open TinylambOpt.ResearchGoals.JitKernels.NullWordFusion

theorem refute : ¬ PaddingBitComplementCount.Goal := by
  intro h
  have hpad : (0 : W) &&& ~~~(window 0) = 0 := by native_decide
  have hh := h 0 (0 : W) (by omega) hpad
  have contra : ¬ ((ords (valid 0 (0 : W))).length = 64 - (ords (0 : W)).length) := by
    native_decide
  exact contra hh

end TinylambOpt.ResearchProofs.JitKernels.Goal_NullWordFusion_PaddingBitComplementCount
