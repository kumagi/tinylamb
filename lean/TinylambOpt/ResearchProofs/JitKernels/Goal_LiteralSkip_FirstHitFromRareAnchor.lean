import TinylambOpt.ResearchGoals.JitKernels.LiteralSkip

/-!
A single anchor candidate need not be an actual match. For needle `[0,1]` and
haystack `[0,9]` the only anchor candidate at offset zero is spurious while
there is no true start at all, so the two `head?` values differ.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.LiteralSkip.Goal_FirstHitFromRareAnchor

open TinylambOpt.ResearchGoals.JitKernels.LiteralSkip

theorem refute : ¬ FirstHitFromRareAnchor.Goal := by
  intro h
  have hl : (cand ([0, 1] : B) ([0, 9] : B) 0).length = 1 := by decide
  have t := h ([0, 1] : B) ([0, 9] : B) 0 (by decide) hl
  have hbad : ¬ ((cand ([0, 1] : B) ([0, 9] : B) 0).head? =
      (starts ([0, 1] : B) ([0, 9] : B)).head?) := by
    decide
  exact hbad t

end TinylambOpt.ResearchProofs.JitKernels.LiteralSkip.Goal_FirstHitFromRareAnchor
