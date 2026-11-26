import TinylambOpt.ResearchGoals.JitKernels.PlanePredicates

set_option maxHeartbeats 4000000
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.PlanePredicates_UnsignedThresholdTruncation
open TinylambOpt.ResearchGoals.JitKernels.PlanePredicates

/-- REFUTED: `foldBits` only inspects bit positions `0 .. w-1`, so an out-of-frame
threshold is silently ignored rather than truncated. With `w = 3`, `x = 0`, `c = 8`
all compared bits agree (`foldBits = false`) yet `0 < 8`. -/
theorem refute : ¬ UnsignedThresholdTruncation.Goal := by
  intro h
  exact absurd (h 0 8 3 (by decide)) (by decide)

end TinylambOpt.ResearchProofs.JitKernels.PlanePredicates_UnsignedThresholdTruncation
