import TinylambOpt.ResearchGoals.JitKernels.NullWordFusion

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_NullWordFusion_SharedUnpackCacheCount

open TinylambOpt.ResearchGoals.JitKernels.NullWordFusion

theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.NullWordFusion.SharedUnpackCacheCount.Goal := by
  intro h
  have hcnt : (ords (1 : W)).length = (ords (2 : W)).length := by native_decide
  have hh := h (1 : W) (2 : W) [0] hcnt
  have contra : ¬ (gather (1 : W) [0] = gather (2 : W) [0]) := by native_decide
  exact contra hh

end TinylambOpt.ResearchProofs.JitKernels.Goal_NullWordFusion_SharedUnpackCacheCount