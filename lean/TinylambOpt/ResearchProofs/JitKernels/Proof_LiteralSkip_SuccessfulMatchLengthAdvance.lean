import TinylambOpt.ResearchGoals.JitKernels.LiteralSkip

/-! Independent proof record for the LiteralSkip.SuccessfulMatchLengthAdvance research goal: the proposed rewrite is unsound and this file exhibits a counterexample. -/

/-!
A self-overlapping needle has matches closer than its own length: `[0,0]`
matches at offsets 0 and 1 of `[0,0,0]`. Advancing by the whole needle length
after a hit would skip the second match.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace Nov26.Proof_LiteralSkip_SuccessfulMatchLengthAdvance
open TinylambOpt.ResearchGoals.JitKernels.LiteralSkip
open TinylambOpt.ResearchGoals.JitKernels.LiteralSkip.SuccessfulMatchLengthAdvance
set_option linter.unusedSimpArgs false

open TinylambOpt.ResearchGoals.JitKernels.LiteralSkip

theorem refute : ¬ Goal := by
  intro h
  have t := h ([0, 0] : B) ([0, 0, 0] : B) 0 1 (by decide)
    (by decide) (by decide) (by omega)
  have hbad : ¬ (0 + ([0, 0] : B).length ≤ 1) := by decide
  exact hbad t

end Nov26.Proof_LiteralSkip_SuccessfulMatchLengthAdvance
