import TinylambOpt.ResearchGoals.JitKernels.BorrowMasks

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_BorrowMasks_BorrowBallotForCompaction

open TinylambOpt.ResearchGoals.JitKernels.BorrowMasks
open TinylambOpt.ResearchGoals.SwarDigitBallot

/-! A zero byte followed by byte 1 borrows into the next lane. The subtract
mask marks both lanes, while the exact zero-byte test marks only the first. -/
theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.BorrowMasks.BorrowBallotForCompaction.Goal := by
  rw [BorrowBallotForCompaction.Goal]
  intro h
  have hh := h (0x0100 : W)
  have hbad : marked (fast (0x0100 : W)) ≠ marked (zeroBytes (0x0100 : W)) := by
    native_decide
  exact hbad hh

end TinylambOpt.ResearchProofs.JitKernels.Goal_BorrowMasks_BorrowBallotForCompaction
