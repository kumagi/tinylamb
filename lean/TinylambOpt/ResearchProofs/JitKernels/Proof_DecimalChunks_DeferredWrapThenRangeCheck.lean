import TinylambOpt.ResearchGoals.JitKernels.DecimalChunks

/-!
Refutation: validating only the wrapped accumulator accepts values that never
fit the real width. With `w = 4`, the string "40" has magnitude 40; its wrap
`40 % 2^(4+1) = 40 % 32 = 8 < 16 = 2^4` passes the delayed check, but
`parse 4 false [52,48] = none` because 40 is outside `[-16, 16)`. A machine
accumulator with deferred range checking is not equivalent to exact parsing.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.DecimalChunks_DeferredWrapThenRangeCheck
open TinylambOpt.ResearchGoals.JitKernels.DecimalChunks

theorem refute : ¬ DeferredWrapThenRangeCheck.Goal := by
  intro h
  exact absurd (h [52,48] 4 (by decide) (by decide) (by decide)) (by decide)

end TinylambOpt.ResearchProofs.JitKernels.DecimalChunks_DeferredWrapThenRangeCheck
