import TinylambOpt.ResearchGoals.JitKernels.LiteralSkip

/-!
Pairwise distinct needle bytes do not make the two endpoint anchors sufficient.
The needle `[0,1,2]` and haystack `[0,9,2]` agree at both endpoints of offset
zero although the interior byte differs, so endpoint-only verification yields a
spurious start.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.LiteralSkip.Goal_EndAnchorsReplaceInterior

open TinylambOpt.ResearchGoals.JitKernels.LiteralSkip

theorem refute : ¬ EndAnchorsReplaceInterior.Goal := by
  intro h
  have t := h ([0, 1, 2] : B) ([0, 9, 2] : B) (by decide) (by decide)
  have hbad : ¬ (((cand ([0, 1, 2] : B) ([0, 9, 2] : B) 0).filter
      (anchor ([0, 1, 2] : B) ([0, 9, 2] : B) (([0, 1, 2] : B).length - 1))) =
      starts ([0, 1, 2] : B) ([0, 9, 2] : B)) := by
    decide
  exact hbad t

end TinylambOpt.ResearchProofs.JitKernels.LiteralSkip.Goal_EndAnchorsReplaceInterior
