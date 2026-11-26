import TinylambOpt.ResearchGoals.JitKernels.BorrowMasks

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.BorrowMasks_BorrowReverseDelimiter
open TinylambOpt.ResearchGoals.JitKernels.BorrowMasks

/--
`BorrowReverseDelimiter.Goal` claims the highest set bit of the subtract mask is
the last zero byte. This is false: a byte equal to 1 above the last zero byte
inherits the borrow chain and becomes the topmost flagged lane.
Witness: `x = 0x0101010101010100` flags every lane under `fast` but only lane 0
under the exact `zeroBytes` mask.
-/
theorem refute : ¬ BorrowReverseDelimiter.Goal := by
  intro h
  exact absurd (h 0x0101010101010100) (by decide)

end TinylambOpt.ResearchProofs.JitKernels.BorrowMasks_BorrowReverseDelimiter
