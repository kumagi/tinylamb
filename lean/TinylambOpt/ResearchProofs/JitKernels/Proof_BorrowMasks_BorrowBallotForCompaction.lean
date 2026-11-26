import TinylambOpt.ResearchGoals.JitKernels.BorrowMasks

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.BorrowMasks_BorrowBallotForCompaction
open TinylambOpt.ResearchGoals.JitKernels.BorrowMasks

/--
`BorrowBallotForCompaction.Goal` claims the cheap subtract-and-mask ballot flags
exactly the zero bytes. This is false: a lane whose byte is 1 directly above a
zero byte inherits the borrow and is flagged spuriously.
Witness: `x = 0x0100` flags lane 1 under `fast` although only lane 0 is zero.
-/
theorem refute : ¬ BorrowBallotForCompaction.Goal := by
  intro h
  exact absurd (h 0x0100) (by decide)

end TinylambOpt.ResearchProofs.JitKernels.BorrowMasks_BorrowBallotForCompaction
