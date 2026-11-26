import TinylambOpt.ResearchGoals.JitKernels.DecimalChunks

/-!
Refutation: the parser rejects the empty string, so trimming every leading
zero from an all-zero input changes the result. With `w = 1`, `digits [48]`
holds, but `parse 1 false ([48].dropWhile (· == 48)) = parse 1 false [] = none`
while `parse 1 false [48] = some 0`. A zero-trimming fast path needs an
all-zeros special case.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.DecimalChunks_TrimZerosPreservesAllZeroInput
open TinylambOpt.ResearchGoals.JitKernels.DecimalChunks

theorem refute : ¬ TrimZerosPreservesAllZeroInput.Goal := by
  intro h
  exact absurd (h 1 false [48] (by decide)) (by decide)

end TinylambOpt.ResearchProofs.JitKernels.DecimalChunks_TrimZerosPreservesAllZeroInput
