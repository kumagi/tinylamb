import TinylambOpt.ResearchGoals.JitKernels.BorrowMasks

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.BorrowMasks_BorrowRankBeforeLimit
open TinylambOpt.ResearchGoals.JitKernels.BorrowMasks

/--
`BorrowRankBeforeLimit.Goal` claims ranks computed by popcounting the subtract
mask match the exact zero-byte mask. This is false: the subtract mask counts
false-positive 1-bytes that merely received a borrow from a lower zero byte.
Witness: `x = 0x0100`, `n = 2`: `fast` flags lanes [0,1], `zeroBytes` flags only [0].
-/
theorem refute : ¬ BorrowRankBeforeLimit.Goal := by
  intro h
  exact absurd (h 0x0100 2 (by decide)) (by decide)

end TinylambOpt.ResearchProofs.JitKernels.BorrowMasks_BorrowRankBeforeLimit
