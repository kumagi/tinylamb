import TinylambOpt.ResearchGoals.JitKernels.CivilTextGathers

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.CivilTextGathers_OffsetHashGrouping
open TinylambOpt.ResearchGoals.JitKernels.CivilTextGathers

/--
`OffsetHashGrouping.Goal` claims equal civil hours are equivalent to equal UTC
hours under one shared minute offset. The forward implication fails: with
`h = h'` but different minutes, the borrow `floor((m - z) / 60)` shifts the UTC
hour.
Witness: `h = h' = 0`, `m = 0`, `m' = 59`, `z = 59`: UTC hours are 23 and 0.
-/
theorem refute : ¬ OffsetHashGrouping.Goal := by
  intro h
  exact absurd
    ((h 0 0 0 0 0 59 59 (by decide) (by decide) (by decide) (by decide)
      (by decide) (by decide) (by decide) (by decide)).1 rfl)
    (by decide)

end TinylambOpt.ResearchProofs.JitKernels.CivilTextGathers_OffsetHashGrouping
