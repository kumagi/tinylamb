import TinylambOpt.ResearchGoals.JitKernels.LiteralSkip

/-!
A self-overlapping needle has matches closer than its own length: `[0,0]`
matches at offsets 0 and 1 of `[0,0,0]`. Advancing by the whole needle length
after a hit would skip the second match.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.LiteralSkip.Goal_SuccessfulMatchLengthAdvance

open TinylambOpt.ResearchGoals.JitKernels.LiteralSkip

theorem refute : ¬ SuccessfulMatchLengthAdvance.Goal := by
  intro h
  have t := h ([0, 0] : B) ([0, 0, 0] : B) 0 1 (by decide)
    (by decide) (by decide) (by omega)
  have hbad : ¬ (0 + ([0, 0] : B).length ≤ 1) := by decide
  exact hbad t

end TinylambOpt.ResearchProofs.JitKernels.LiteralSkip.Goal_SuccessfulMatchLengthAdvance
