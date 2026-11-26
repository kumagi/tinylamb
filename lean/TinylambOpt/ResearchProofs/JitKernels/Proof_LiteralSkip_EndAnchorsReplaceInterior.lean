import TinylambOpt.ResearchGoals.JitKernels.LiteralSkip

/-! Independent proof record for the LiteralSkip.EndAnchorsReplaceInterior research goal: the proposed rewrite is unsound and this file exhibits a counterexample. -/

/-!
Pairwise distinct needle bytes do not make the two endpoint anchors sufficient.
The needle `[0,1,2]` and haystack `[0,9,2]` agree at both endpoints of offset
zero although the interior byte differs, so endpoint-only verification yields a
spurious start.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace Nov26.Proof_LiteralSkip_EndAnchorsReplaceInterior
open TinylambOpt.ResearchGoals.JitKernels.LiteralSkip
open TinylambOpt.ResearchGoals.JitKernels.LiteralSkip.EndAnchorsReplaceInterior
set_option linter.unusedSimpArgs false

open TinylambOpt.ResearchGoals.JitKernels.LiteralSkip

theorem refute : ¬ Goal := by
  intro h
  have t := h ([0, 1, 2] : B) ([0, 9, 2] : B) (by decide) (by decide)
  have hbad : ¬ (((cand ([0, 1, 2] : B) ([0, 9, 2] : B) 0).filter
      (anchor ([0, 1, 2] : B) ([0, 9, 2] : B) (([0, 1, 2] : B).length - 1))) =
      starts ([0, 1, 2] : B) ([0, 9, 2] : B)) := by
    decide
  exact hbad t

end Nov26.Proof_LiteralSkip_EndAnchorsReplaceInterior
