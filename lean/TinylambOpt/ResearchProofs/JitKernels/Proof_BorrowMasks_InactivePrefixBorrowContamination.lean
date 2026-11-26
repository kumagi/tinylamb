import TinylambOpt.ResearchGoals.JitKernels.BorrowMasks

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.BorrowMasks_InactivePrefixBorrowContamination
open TinylambOpt.ResearchGoals.JitKernels.BorrowMasks

/--
`InactivePrefixBorrowContamination.Goal` claims that discarding subtract-mask bits
below `start` still recovers the first true zero byte of the suffix. This is false:
the first surviving hit can be the false-positive 1-byte directly above a zero
byte that was itself masked away.
Witness: `x = 0x0100`, `start = 1`: the subtract mask resumes at lane 1, the exact
mask at lane 2.
-/
theorem refute : ¬ InactivePrefixBorrowContamination.Goal := by
  intro h
  exact absurd (h 0x0100 1) (by decide)

end TinylambOpt.ResearchProofs.JitKernels.BorrowMasks_InactivePrefixBorrowContamination
