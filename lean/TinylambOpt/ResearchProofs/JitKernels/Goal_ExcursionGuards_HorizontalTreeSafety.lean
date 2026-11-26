import TinylambOpt.ResearchGoals.JitKernels.ExcursionGuards

set_option maxHeartbeats 0
set_option maxRecDepth 10000

/-! Refutation: a safe whole-chunk scan does not certify a subtotal restarted
from zero. A preceding negative prefix can mask a middle segment that would by
itself exceed the upper bound, so horizontal tree reassociation is unsafe. -/

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_ExcursionGuards_HorizontalTreeSafety

open TinylambOpt.ResearchGoals.JitKernels.ExcursionGuards

theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.ExcursionGuards.HorizontalTreeSafety.Goal := by
  intro h
  have hbad := h (-150) 150 0 [-100] [200] [-100] (by omega) (by omega) (by native_decide)
  have hc : safe (-150) 150 0 [200] ≠ true := by native_decide
  exact hc hbad

end TinylambOpt.ResearchProofs.JitKernels.Goal_ExcursionGuards_HorizontalTreeSafety
