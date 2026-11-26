import TinylambOpt.ResearchGoals.JitKernels.LiteralSkip

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_LiteralSkip_SuccessfulMatchLengthAdvance

open TinylambOpt.ResearchGoals.JitKernels.LiteralSkip

/-! A self-overlapping needle has matches closer than its own length. -/
theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.LiteralSkip.SuccessfulMatchLengthAdvance.Goal := by
  rw [SuccessfulMatchLengthAdvance.Goal]
  intro h
  have hh := h ([0,0] : B) ([0,0,0] : B) 0 1 (by native_decide)
    (by native_decide) (by native_decide) (by omega)
  have hbad : ¬ (0 + ([0,0] : B).length ≤ 1) := by native_decide
  exact hbad hh

end TinylambOpt.ResearchProofs.JitKernels.Goal_LiteralSkip_SuccessfulMatchLengthAdvance
