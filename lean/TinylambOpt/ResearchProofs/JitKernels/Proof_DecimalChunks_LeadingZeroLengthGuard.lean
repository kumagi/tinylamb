import TinylambOpt.ResearchGoals.JitKernels.DecimalChunks

/-!
Refutation: a length test on the raw digit string is unsound when leading
zeros are present. With `w = 1` the string "01" has length 2, so
`10^(2-1) = 10 > 2 = 2^1`, yet its value 1 satisfies `-2 ≤ 1 < 2` and
`parse 1 false [48,49] = some 1` succeeds. Rejecting purely on digit count
would wrongly discard it.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.DecimalChunks_LeadingZeroLengthGuard
open TinylambOpt.ResearchGoals.JitKernels.DecimalChunks

theorem refute : ¬ LeadingZeroLengthGuard.Goal := by
  intro h
  exact absurd (h 1 [48,49] (by decide) (by decide)) (by decide)

end TinylambOpt.ResearchProofs.JitKernels.DecimalChunks_LeadingZeroLengthGuard
