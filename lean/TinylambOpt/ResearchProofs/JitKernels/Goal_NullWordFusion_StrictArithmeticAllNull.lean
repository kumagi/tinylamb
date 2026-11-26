import TinylambOpt.ResearchGoals.JitKernels.NullWordFusion

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_NullWordFusion_StrictArithmeticAllNull

open TinylambOpt.ResearchGoals.JitKernels.NullWordFusion

theorem refute : ¬ StrictArithmeticAllNull.Goal := by
  intro h
  have ha : (1 : W) &&& window 3 ≠ 0 := by native_decide
  have hb : (2 : W) &&& window 3 ≠ 0 := by native_decide
  have hh := h (1 : W) (2 : W) 3 ha hb
  have contra : ¬ (valid 3 ((1 : W) ||| (2 : W)) = 0) := by native_decide
  exact contra hh

end TinylambOpt.ResearchProofs.JitKernels.Goal_NullWordFusion_StrictArithmeticAllNull
